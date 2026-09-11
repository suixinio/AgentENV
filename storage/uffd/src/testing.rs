//! Helpers for playing the VMM side: an anonymous mapping registered with a
//! userfaultfd this process created. Used by the test suite and the
//! self-test.

use std::io;
use std::ptr;

use anyhow::{Context, Result};

use crate::handshake::GuestRegionUffdMapping;
use crate::proto::{Uffd, UFFDIO_REGISTER_MODE_MISSING};

/// An anonymous private mapping of `len` bytes.
pub struct AnonRegion {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: the mapping is process-global memory; concurrent readers are the
// point of the tests, and the type never hands out `&mut`.
unsafe impl Send for AnonRegion {}
unsafe impl Sync for AnonRegion {}

impl AnonRegion {
    pub fn new(len: usize) -> Result<Self> {
        // SAFETY: anonymous mapping with no file and no fixed address.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error()).context("mmap an anonymous region");
        }
        Ok(Self {
            ptr: ptr.cast(),
            len,
        })
    }

    /// A 2 MiB hugetlbfs mapping. The pool is charged at map time (no
    /// `MAP_NORESERVE`), so a region that maps can be touched; a host with
    /// no pool answers `ENOMEM` here rather than `SIGBUS` later.
    pub fn new_hugetlb(len: usize) -> Result<Self> {
        // SAFETY: anonymous mapping with no file and no fixed address.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error()).context("mmap a hugetlb region");
        }
        Ok(Self {
            ptr: ptr.cast(),
            len,
        })
    }

    pub fn addr(&self) -> u64 {
        self.ptr as u64
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Registers the whole region for missing-page faults; `features` go to
    /// `UFFDIO_API` first. Returns the granted feature set.
    pub fn register(&self, uffd: &Uffd, features: u64) -> Result<u64> {
        let granted = uffd.api(features).context("UFFDIO_API")?;
        uffd.register(self.addr(), self.len as u64, UFFDIO_REGISTER_MODE_MISSING)
            .context("UFFDIO_REGISTER")?;
        Ok(granted)
    }

    /// The handshake mapping for this region at image offset `offset`.
    pub fn mapping(&self, offset: u64, page_size: u64) -> GuestRegionUffdMapping {
        GuestRegionUffdMapping {
            base_host_virt_addr: self.addr(),
            size: self.len as u64,
            offset,
            page_size,
            page_size_kib: page_size,
            guest_phys_addr: 0,
        }
    }

    /// Reads one byte, faulting the page in if needed.
    pub fn read_byte(&self, at: usize) -> u8 {
        assert!(at < self.len);
        // SAFETY: in bounds of the mapping.
        unsafe { ptr::read_volatile(self.ptr.add(at)) }
    }

    /// Copies `len` bytes out, faulting as needed.
    pub fn read(&self, at: usize, len: usize) -> Vec<u8> {
        assert!(at + len <= self.len);
        let mut out = vec![0u8; len];
        // SAFETY: in bounds of the mapping.
        unsafe { ptr::copy_nonoverlapping(self.ptr.add(at), out.as_mut_ptr(), len) };
        out
    }

    pub fn write_byte(&self, at: usize, value: u8) {
        assert!(at < self.len);
        // SAFETY: in bounds of the mapping.
        unsafe { ptr::write_volatile(self.ptr.add(at), value) }
    }

    /// `madvise(MADV_DONTNEED)` on `[at, at + len)`, which the kernel reports
    /// as `UFFD_EVENT_REMOVE` when the feature is on.
    pub fn discard(&self, at: usize, len: usize) -> Result<()> {
        assert!(at + len <= self.len);
        // SAFETY: in bounds of the mapping.
        if unsafe { libc::madvise(self.ptr.add(at).cast(), len, libc::MADV_DONTNEED) } != 0 {
            return Err(io::Error::last_os_error()).context("madvise(MADV_DONTNEED)");
        }
        Ok(())
    }
}

impl Drop for AnonRegion {
    fn drop(&mut self) {
        // SAFETY: the mapping was created by `new` with this length.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// Creates a userfaultfd for a test, first as this account allows for
/// user-mode faults, then with the full syscall. `None` when neither works.
pub fn create_uffd_for_test() -> Option<(Uffd, io::Error)> {
    match Uffd::create(true) {
        Ok(uffd) => Some((uffd, io::Error::from_raw_os_error(0))),
        Err(first) => match Uffd::create(false) {
            Ok(uffd) => Some((uffd, first)),
            Err(_) => None,
        },
    }
}
