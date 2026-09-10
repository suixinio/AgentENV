use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

use crate::{IOBuffer, UVMUblkTarget, UblkDescOperation};

const DEFAULT_PHYSICAL_BS_SHIFT: u8 = 12;
/// ublk describes device size, request offsets and lengths in 512-byte sectors
/// whatever the logical block size is.
const SECTOR_SHIFT: u8 = 9;

/// An in-memory block target: what a measurement run exposes when it wants the
/// transport's cost without an image underneath it.
#[derive(Debug)]
pub struct MemTarget {
    data: Mutex<Vec<u8>>,
    logical_bs_shift: u8,
    physical_bs_shift: u8,
    dev_sectors: u64,
    largest_request: AtomicUsize,
    flushes: AtomicUsize,
}

impl MemTarget {
    /// `block_size` is the logical block size and must be a power of two in
    /// [512, 4096]; `size_bytes` must be a whole number of those blocks.
    pub fn new(size_bytes: usize, block_size: u32) -> Result<Self> {
        anyhow::ensure!(
            (512..=4096).contains(&block_size) && block_size.is_power_of_two(),
            "ublk block size must be a power of two in [512, 4096], got {block_size}"
        );
        anyhow::ensure!(
            size_bytes > 0 && size_bytes.is_multiple_of(block_size as usize),
            "ublk device size {size_bytes} must be a non-zero multiple of the {block_size} \
             byte block"
        );
        let logical_bs_shift = block_size.trailing_zeros() as u8;
        Ok(Self {
            data: Mutex::new(vec![0u8; size_bytes]),
            logical_bs_shift,
            physical_bs_shift: DEFAULT_PHYSICAL_BS_SHIFT.max(logical_bs_shift),
            dev_sectors: (size_bytes as u64) >> SECTOR_SHIFT,
            largest_request: AtomicUsize::new(0),
            flushes: AtomicUsize::new(0),
        })
    }

    /// A copy of the backing bytes, for asserting what reached the target.
    pub fn snapshot(&self, offset: u64, len: usize) -> Vec<u8> {
        let data = self.data.lock();
        let start = offset as usize;
        data[start..start + len].to_vec()
    }

    pub fn fill(&self, offset: u64, bytes: &[u8]) {
        let mut data = self.data.lock();
        let start = offset as usize;
        data[start..start + bytes.len()].copy_from_slice(bytes);
    }

    /// The longest single read or write the kernel has issued so far.
    pub fn largest_request(&self) -> usize {
        self.largest_request.load(Ordering::Relaxed)
    }

    pub fn flushes(&self) -> usize {
        self.flushes.load(Ordering::Relaxed)
    }

    fn dev_bytes(&self) -> u64 {
        self.dev_sectors << SECTOR_SHIFT
    }

    /// The kernel describes a flush with `start_sector` -1 and no sectors: it
    /// names the whole device, not a range, so it never reaches this.
    fn request_meta(
        &self,
        io_desc: ublk_sys::ublksrv_io_desc,
    ) -> std::result::Result<(UblkDescOperation, u64, usize), i32> {
        let op = UblkDescOperation::try_from(io_desc.op_flags & 0xff).map_err(|_| -libc::EINVAL)?;
        if matches!(op, UblkDescOperation::Flush) {
            return Ok((op, 0, 0));
        }
        let offset = io_desc.start_sector << SECTOR_SHIFT;
        let len = (io_desc.nr_sectors as u64) << SECTOR_SHIFT;
        let end = offset.checked_add(len).ok_or(-libc::EINVAL)?;
        if end > self.dev_bytes() {
            return Err(-libc::EINVAL);
        }
        let len = usize::try_from(len).map_err(|_| -libc::EINVAL)?;
        Ok((op, offset, len))
    }
}

fn build_ublk_params(
    dev_sectors: u64,
    logical_bs_shift: u8,
    physical_bs_shift: u8,
    max_io_buf_bytes: u32,
) -> ublk_sys::ublk_params {
    let logical_block_size = 1u32 << logical_bs_shift;
    ublk_sys::ublk_params {
        types: ublk_sys::UBLK_PARAM_TYPE_BASIC | ublk_sys::UBLK_PARAM_TYPE_DISCARD,
        len: size_of::<ublk_sys::ublk_params>() as u32,
        basic: ublk_sys::ublk_param_basic {
            // Without this the kernel reports write-through and never sends a
            // Flush, so the measured path would differ from the image one.
            attrs: ublk_sys::UBLK_ATTR_VOLATILE_CACHE,
            logical_bs_shift,
            physical_bs_shift,
            io_min_shift: logical_bs_shift,
            io_opt_shift: physical_bs_shift,
            max_sectors: max_io_buf_bytes >> SECTOR_SHIFT,
            dev_sectors,
            ..Default::default()
        },
        discard: ublk_sys::ublk_param_discard {
            discard_alignment: logical_block_size,
            discard_granularity: logical_block_size,
            max_discard_sectors: max_io_buf_bytes >> SECTOR_SHIFT,
            max_write_zeroes_sectors: 0,
            max_discard_segments: 1,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[async_trait(?Send)]
impl UVMUblkTarget for MemTarget {
    const DEV_NAME: &str = "mem";

    async fn init(&self, _qid: u16, _io_ring: &AsyncIoRing) -> Result<()> {
        Ok(())
    }

    fn per_slot_extra_buf_len(&self) -> Option<usize> {
        None
    }

    fn ublk_dev_params(&self, dev_info: &ublk_sys::ublksrv_ctrl_dev_info) -> ublk_sys::ublk_params {
        build_ublk_params(
            self.dev_sectors,
            self.logical_bs_shift,
            self.physical_bs_shift,
            dev_info.max_io_buf_bytes,
        )
    }

    async fn handle_io_request(
        self: &Arc<Self>,
        qid: u16,
        tag: u16,
        io_desc: ublk_sys::ublksrv_io_desc,
        buf: &mut IOBuffer,
        _extra: Option<&mut IOBuffer>,
        _io_ring: &AsyncIoRing,
    ) -> i32 {
        let (op, offset, len) = match self.request_meta(io_desc) {
            Ok(meta) => meta,
            Err(err) => {
                tracing::error!(qid, tag, ?io_desc, "invalid mem ublk request");
                return err;
            }
        };
        let start = offset as usize;

        match op {
            UblkDescOperation::Read => {
                if len == 0 {
                    return 0;
                }
                let IOBuffer::User(user_buf) = buf else {
                    return -libc::EINVAL;
                };
                self.largest_request.fetch_max(len, Ordering::Relaxed);
                user_buf
                    .subslice_mut(0, len)
                    .copy_from_slice(&self.data.lock()[start..start + len]);
                len as i32
            }
            UblkDescOperation::Write => {
                if len == 0 {
                    return 0;
                }
                let IOBuffer::User(user_buf) = buf else {
                    return -libc::EINVAL;
                };
                self.largest_request.fetch_max(len, Ordering::Relaxed);
                self.data.lock()[start..start + len].copy_from_slice(user_buf.subslice(0, len));
                len as i32
            }
            UblkDescOperation::Flush => {
                self.flushes.fetch_add(1, Ordering::Relaxed);
                0
            }
            UblkDescOperation::Discard => {
                self.data.lock()[start..start + len].fill(0);
                0
            }
            _ => {
                tracing::error!(
                    qid,
                    tag,
                    ?op,
                    "mem ublk target does not support this operation"
                );
                -libc::EINVAL
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{UVMUblkTarget, UserBuffer};
    use storage_util::io_ring::AsyncIoRingBuilder;
    use ublk_sys::ublksrv_io_desc;

    fn ring() -> AsyncIoRing {
        AsyncIoRingBuilder::new()
            .nr_sparse_buffer(4)
            .nr_sparse_file(2)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring")
    }

    async fn user_buffer(ring: &AsyncIoRing, len: usize) -> IOBuffer {
        IOBuffer::User(
            UserBuffer::new(ring.clone(), len, 512)
                .await
                .expect("allocate io buffer"),
        )
    }

    fn desc(op: u32, start_sector: u64, nr_sectors: u32, buf: &IOBuffer) -> ublksrv_io_desc {
        ublksrv_io_desc {
            op_flags: op,
            nr_sectors,
            start_sector,
            addr: buf.uring_buf_idx() as u64,
        }
    }

    #[test]
    fn a_size_or_block_size_the_kernel_refuses_is_rejected_at_construction() {
        assert!(MemTarget::new(1 << 20, 4096).is_ok());
        assert!(MemTarget::new(1 << 20, 512).is_ok());
        assert!(MemTarget::new(1 << 20, 256).is_err());
        assert!(MemTarget::new(1 << 20, 8192).is_err());
        assert!(MemTarget::new(1 << 20, 768).is_err());
        assert!(MemTarget::new(0, 4096).is_err());
        assert!(MemTarget::new(4096 + 512, 4096).is_err());
    }

    #[test]
    fn the_params_describe_the_geometry_the_target_was_built_with() {
        let params = build_ublk_params(2048, 9, 12, 512 * 1024);
        assert_eq!(
            params.types,
            ublk_sys::UBLK_PARAM_TYPE_BASIC | ublk_sys::UBLK_PARAM_TYPE_DISCARD
        );
        assert_eq!(params.basic.logical_bs_shift, 9);
        assert_eq!(params.basic.physical_bs_shift, 12);
        assert_eq!(params.basic.dev_sectors, 2048);
        assert_eq!(params.basic.max_sectors, 1024);
        assert_eq!(
            params.basic.attrs & ublk_sys::UBLK_ATTR_VOLATILE_CACHE,
            ublk_sys::UBLK_ATTR_VOLATILE_CACHE,
            "a guest fsync only reaches the flush handler when the cache says it is volatile"
        );
        assert_eq!(params.discard.discard_granularity, 512);
        assert_eq!(params.discard.max_discard_segments, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_write_is_visible_to_a_later_read_at_the_same_offset() {
        let target = Arc::new(MemTarget::new(1 << 20, 4096).unwrap());
        let ring = ring();
        let mut buf = user_buffer(&ring, 4096).await;

        if let IOBuffer::User(user) = &mut buf {
            user.subslice_mut(0, 4096).fill(0x5A);
        }
        let write = desc(ublk_sys::UBLK_IO_OP_WRITE, 8, 8, &buf);
        assert_eq!(
            target
                .handle_io_request(0, 0, write, &mut buf, None, &ring)
                .await,
            4096
        );
        assert_eq!(target.snapshot(4096, 4096), vec![0x5A; 4096]);
        assert_eq!(target.snapshot(0, 4096), vec![0u8; 4096]);

        if let IOBuffer::User(user) = &mut buf {
            user.subslice_mut(0, 4096).fill(0);
        }
        let read = desc(ublk_sys::UBLK_IO_OP_READ, 8, 8, &buf);
        assert_eq!(
            target
                .handle_io_request(0, 0, read, &mut buf, None, &ring)
                .await,
            4096
        );
        if let IOBuffer::User(user) = &buf {
            assert!(user.subslice(0, 4096).iter().all(|&byte| byte == 0x5A));
        }
        assert_eq!(target.largest_request(), 4096);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_request_past_the_end_of_the_device_is_refused_as_einval() {
        let target = Arc::new(MemTarget::new(8192, 4096).unwrap());
        let ring = ring();
        let mut buf = user_buffer(&ring, 4096).await;

        let past_end = desc(ublk_sys::UBLK_IO_OP_READ, 16, 8, &buf);
        assert_eq!(
            target
                .handle_io_request(0, 0, past_end, &mut buf, None, &ring)
                .await,
            -libc::EINVAL
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_flush_descriptor_names_the_whole_device_rather_than_a_range() {
        let target = Arc::new(MemTarget::new(8192, 4096).unwrap());
        let ring = ring();
        let mut buf = user_buffer(&ring, 4096).await;

        // The kernel spells a flush with start_sector -1 and no sectors; a
        // target that reads those as a range refuses every guest fsync.
        let flush = ublksrv_io_desc {
            op_flags: ublk_sys::UBLK_IO_OP_FLUSH,
            nr_sectors: 0,
            start_sector: u64::MAX,
            addr: buf.uring_buf_idx() as u64,
        };
        assert_eq!(
            target
                .handle_io_request(0, 0, flush, &mut buf, None, &ring)
                .await,
            0
        );
        assert_eq!(target.flushes(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_discard_zeroes_only_the_named_range() {
        let target = Arc::new(MemTarget::new(1 << 20, 4096).unwrap());
        let ring = ring();
        let mut buf = user_buffer(&ring, 4096).await;
        target.fill(0, &[0xFF; 3 * 4096]);

        let discard = desc(ublk_sys::UBLK_IO_OP_DISCARD, 8, 8, &buf);
        assert_eq!(
            target
                .handle_io_request(0, 0, discard, &mut buf, None, &ring)
                .await,
            0
        );
        assert!(target.snapshot(0, 4096).iter().all(|&byte| byte == 0xFF));
        assert!(target.snapshot(4096, 4096).iter().all(|&byte| byte == 0));
        assert!(target.snapshot(8192, 4096).iter().all(|&byte| byte == 0xFF));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_operation_the_target_does_not_implement_is_refused_as_einval() {
        let target = Arc::new(MemTarget::new(8192, 4096).unwrap());
        let ring = ring();
        let mut buf = user_buffer(&ring, 4096).await;

        let write_zeroes = desc(ublk_sys::UBLK_IO_OP_WRITE_ZEROES, 0, 1, &buf);
        assert_eq!(
            target
                .handle_io_request(0, 0, write_zeroes, &mut buf, None, &ring)
                .await,
            -libc::EINVAL
        );
    }
}
