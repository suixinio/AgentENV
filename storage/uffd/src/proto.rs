//! The userfaultfd ABI as the handler side uses it: ioctl numbers, the
//! message layout and a thin wrapper over the descriptor.
//!
//! Firecracker creates the descriptor, calls `UFFDIO_API` and registers the
//! guest regions before handing it over; the handler only reads events and
//! installs pages. The `api`/`register` calls exist for tests and the
//! self-test, which play the Firecracker side themselves.

use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

const IOC_WRITE: u32 = 1;
const IOC_READ: u32 = 2;

// asm-generic/ioctl.h: nr 8 bits, type 8 bits, size 14 bits, dir 2 bits.
const fn ioc(dir: u32, typ: u32, nr: u32, size: usize) -> libc::c_ulong {
    ((dir << 30) | ((size as u32) << 16) | (typ << 8) | nr) as libc::c_ulong
}

const UFFDIO: u32 = 0xAA;

/// `UFFD_API`, the protocol version handed to `UFFDIO_API`.
pub const UFFD_API: u64 = 0xAA;

pub const UFFD_FEATURE_PAGEFAULT_FLAG_WP: u64 = 1 << 0;
pub const UFFD_FEATURE_EVENT_FORK: u64 = 1 << 1;
pub const UFFD_FEATURE_EVENT_REMAP: u64 = 1 << 2;
pub const UFFD_FEATURE_EVENT_REMOVE: u64 = 1 << 3;
pub const UFFD_FEATURE_MISSING_HUGETLBFS: u64 = 1 << 4;
pub const UFFD_FEATURE_MISSING_SHMEM: u64 = 1 << 5;
pub const UFFD_FEATURE_EVENT_UNMAP: u64 = 1 << 6;
pub const UFFD_FEATURE_WP_ASYNC: u64 = 1 << 15;

pub const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
pub const UFFD_EVENT_FORK: u8 = 0x13;
pub const UFFD_EVENT_REMAP: u8 = 0x14;
pub const UFFD_EVENT_REMOVE: u8 = 0x15;
pub const UFFD_EVENT_UNMAP: u8 = 0x16;

pub const UFFD_PAGEFAULT_FLAG_WRITE: u64 = 1 << 0;
pub const UFFD_PAGEFAULT_FLAG_WP: u64 = 1 << 1;
pub const UFFD_PAGEFAULT_FLAG_MINOR: u64 = 1 << 2;

pub const UFFDIO_REGISTER_MODE_MISSING: u64 = 1 << 0;
pub const UFFDIO_REGISTER_MODE_WP: u64 = 1 << 1;
pub const UFFDIO_REGISTER_MODE_MINOR: u64 = 1 << 2;

pub const UFFDIO_COPY_MODE_DONTWAKE: u64 = 1 << 0;
pub const UFFDIO_COPY_MODE_WP: u64 = 1 << 1;
pub const UFFDIO_ZEROPAGE_MODE_DONTWAKE: u64 = 1 << 0;
pub const UFFDIO_WRITEPROTECT_MODE_WP: u64 = 1 << 0;
pub const UFFDIO_WRITEPROTECT_MODE_DONTWAKE: u64 = 1 << 1;

/// `userfaultfd(2)` flag: only user-mode faults are handled, which lets an
/// unprivileged caller create the descriptor while
/// `vm.unprivileged_userfaultfd` is 0.
pub const UFFD_USER_MODE_ONLY: libc::c_int = 1;

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioApi {
    pub api: u64,
    pub features: u64,
    pub ioctls: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioRange {
    pub start: u64,
    pub len: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioRegister {
    pub range: UffdioRange,
    pub mode: u64,
    pub ioctls: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioCopy {
    pub dst: u64,
    pub src: u64,
    pub len: u64,
    pub mode: u64,
    pub copy: i64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioZeropage {
    pub range: UffdioRange,
    pub mode: u64,
    pub zeropage: i64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdioWriteprotect {
    pub range: UffdioRange,
    pub mode: u64,
}

const UFFDIO_API_REQ: libc::c_ulong =
    ioc(IOC_READ | IOC_WRITE, UFFDIO, 0x3F, size_of::<UffdioApi>());
const UFFDIO_REGISTER_REQ: libc::c_ulong = ioc(
    IOC_READ | IOC_WRITE,
    UFFDIO,
    0x00,
    size_of::<UffdioRegister>(),
);
const UFFDIO_UNREGISTER_REQ: libc::c_ulong = ioc(IOC_READ, UFFDIO, 0x01, size_of::<UffdioRange>());
const UFFDIO_WAKE_REQ: libc::c_ulong = ioc(IOC_READ, UFFDIO, 0x02, size_of::<UffdioRange>());
const UFFDIO_COPY_REQ: libc::c_ulong =
    ioc(IOC_READ | IOC_WRITE, UFFDIO, 0x03, size_of::<UffdioCopy>());
const UFFDIO_ZEROPAGE_REQ: libc::c_ulong = ioc(
    IOC_READ | IOC_WRITE,
    UFFDIO,
    0x04,
    size_of::<UffdioZeropage>(),
);
const UFFDIO_WRITEPROTECT_REQ: libc::c_ulong = ioc(
    IOC_READ | IOC_WRITE,
    UFFDIO,
    0x06,
    size_of::<UffdioWriteprotect>(),
);
// /dev/userfaultfd: USERFAULTFD_IOC_NEW is _IO(0xAA, 0x00).
const USERFAULTFD_IOC_NEW: libc::c_ulong = ioc(0, UFFDIO, 0x00, 0);

/// `struct uffd_msg`: one 32-byte record per event on the descriptor.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct UffdMsg {
    pub event: u8,
    reserved1: u8,
    reserved2: u16,
    reserved3: u32,
    arg: [u64; 3],
}

/// A decoded event. `Other` carries the event type the handler does not act
/// on (fork, remap, unmap are never enabled by Firecracker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Pagefault { flags: u64, address: u64 },
    Remove { start: u64, end: u64 },
    Other(u8),
}

impl UffdMsg {
    pub fn decode(&self) -> Event {
        match self.event {
            UFFD_EVENT_PAGEFAULT => Event::Pagefault {
                flags: self.arg[0],
                address: self.arg[1],
            },
            UFFD_EVENT_REMOVE => Event::Remove {
                start: self.arg[0],
                end: self.arg[1],
            },
            other => Event::Other(other),
        }
    }
}

const _: () = assert!(size_of::<UffdMsg>() == 32);

/// An owned userfaultfd descriptor.
#[derive(Debug)]
pub struct Uffd {
    fd: OwnedFd,
}

impl AsRawFd for Uffd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl From<OwnedFd> for Uffd {
    fn from(fd: OwnedFd) -> Self {
        Self { fd }
    }
}

impl Uffd {
    /// Creates a descriptor with `userfaultfd(2)`, falling back to
    /// `/dev/userfaultfd` when the syscall is refused. `user_mode_only` is
    /// what an unprivileged caller needs under `vm.unprivileged_userfaultfd=0`;
    /// Firecracker itself never sets it.
    pub fn create(user_mode_only: bool) -> io::Result<Self> {
        let mut flags = libc::O_CLOEXEC | libc::O_NONBLOCK;
        if user_mode_only {
            flags |= UFFD_USER_MODE_ONLY;
        }
        // SAFETY: plain syscall with integer arguments.
        let fd = unsafe { libc::syscall(libc::SYS_userfaultfd, flags) };
        if fd >= 0 {
            // SAFETY: a freshly created descriptor nobody else owns.
            return Ok(Self {
                fd: unsafe { OwnedFd::from_raw_fd(fd as RawFd) },
            });
        }
        let syscall_err = io::Error::last_os_error();
        let dev = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/userfaultfd")
        {
            Ok(dev) => dev,
            Err(_) => return Err(syscall_err),
        };
        // SAFETY: ioctl on an open device with no argument.
        let fd = unsafe { libc::ioctl(dev.as_raw_fd(), USERFAULTFD_IOC_NEW, flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a freshly created descriptor nobody else owns.
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd as RawFd) },
        })
    }

    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }

    pub fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }

    /// `UFFDIO_API`; returns the feature set the kernel granted.
    pub fn api(&self, features: u64) -> io::Result<u64> {
        let mut req = UffdioApi {
            api: UFFD_API,
            features,
            ioctls: 0,
        };
        self.ioctl(UFFDIO_API_REQ, &mut req as *mut _ as *mut libc::c_void)?;
        Ok(req.features)
    }

    /// `UFFDIO_REGISTER` of `[start, start + len)` with `mode` bits.
    pub fn register(&self, start: u64, len: u64, mode: u64) -> io::Result<()> {
        let mut req = UffdioRegister {
            range: UffdioRange { start, len },
            mode,
            ioctls: 0,
        };
        self.ioctl(UFFDIO_REGISTER_REQ, &mut req as *mut _ as *mut libc::c_void)
    }

    pub fn unregister(&self, start: u64, len: u64) -> io::Result<()> {
        let mut req = UffdioRange { start, len };
        self.ioctl(
            UFFDIO_UNREGISTER_REQ,
            &mut req as *mut _ as *mut libc::c_void,
        )
    }

    /// `UFFDIO_COPY` of `len` bytes from `src` to the registered address
    /// `dst`. A short copy is reported as `EAGAIN`, which is also what the
    /// kernel answers while the address space is changing; the caller retries.
    pub fn copy(&self, dst: u64, src: *const u8, len: u64, mode: u64) -> io::Result<()> {
        let mut req = UffdioCopy {
            dst,
            src: src as u64,
            len,
            mode,
            copy: 0,
        };
        match self.ioctl(UFFDIO_COPY_REQ, &mut req as *mut _ as *mut libc::c_void) {
            Ok(()) if req.copy == len as i64 => Ok(()),
            Ok(()) => Err(io::Error::from_raw_os_error(libc::EAGAIN)),
            Err(err) => Err(err),
        }
    }

    /// `UFFDIO_ZEROPAGE`: maps the shared zero page (4 KiB pages only).
    pub fn zeropage(&self, start: u64, len: u64, mode: u64) -> io::Result<()> {
        let mut req = UffdioZeropage {
            range: UffdioRange { start, len },
            mode,
            zeropage: 0,
        };
        match self.ioctl(UFFDIO_ZEROPAGE_REQ, &mut req as *mut _ as *mut libc::c_void) {
            Ok(()) if req.zeropage == len as i64 => Ok(()),
            Ok(()) => Err(io::Error::from_raw_os_error(libc::EAGAIN)),
            Err(err) => Err(err),
        }
    }

    pub fn wake(&self, start: u64, len: u64) -> io::Result<()> {
        let mut req = UffdioRange { start, len };
        self.ioctl(UFFDIO_WAKE_REQ, &mut req as *mut _ as *mut libc::c_void)
    }

    /// `UFFDIO_WRITEPROTECT`: `mode` with `UFFDIO_WRITEPROTECT_MODE_WP` arms,
    /// 0 disarms and wakes.
    pub fn write_protect(&self, start: u64, len: u64, mode: u64) -> io::Result<()> {
        let mut req = UffdioWriteprotect {
            range: UffdioRange { start, len },
            mode,
        };
        self.ioctl(
            UFFDIO_WRITEPROTECT_REQ,
            &mut req as *mut _ as *mut libc::c_void,
        )
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        // SAFETY: fcntl on a descriptor this struct owns.
        let flags = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let flags = if nonblocking {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        // SAFETY: as above.
        if unsafe { libc::fcntl(self.as_raw_fd(), libc::F_SETFL, flags) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reads as many events as fit in `buf`; returns how many. A non-blocking
    /// descriptor with nothing pending answers `WouldBlock`.
    pub fn read_events(&self, buf: &mut [UffdMsg]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // SAFETY: the buffer is a contiguous array of 32-byte records the
        // kernel fills whole records into.
        let n = unsafe {
            libc::read(
                self.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                std::mem::size_of_val(buf),
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize / size_of::<UffdMsg>())
    }

    fn ioctl(&self, req: libc::c_ulong, arg: *mut libc::c_void) -> io::Result<()> {
        // SAFETY: the request number matches the argument struct's layout.
        if unsafe { libc::ioctl(self.as_raw_fd(), req, arg) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioctl_numbers_match_the_kernel_header() {
        // Values from <linux/userfaultfd.h> on x86_64 and aarch64.
        assert_eq!(UFFDIO_API_REQ, 0xc018_aa3f);
        assert_eq!(UFFDIO_REGISTER_REQ, 0xc020_aa00);
        assert_eq!(UFFDIO_UNREGISTER_REQ, 0x8010_aa01);
        assert_eq!(UFFDIO_WAKE_REQ, 0x8010_aa02);
        assert_eq!(UFFDIO_COPY_REQ, 0xc028_aa03);
        assert_eq!(UFFDIO_ZEROPAGE_REQ, 0xc020_aa04);
        assert_eq!(UFFDIO_WRITEPROTECT_REQ, 0xc018_aa06);
        assert_eq!(USERFAULTFD_IOC_NEW, 0xaa00);
    }

    #[test]
    fn messages_decode_by_event_type() {
        let mut msg = UffdMsg {
            event: UFFD_EVENT_PAGEFAULT,
            ..UffdMsg::default()
        };
        msg.arg = [UFFD_PAGEFAULT_FLAG_WRITE, 0x7f00_0000_1234, 0];
        assert_eq!(
            msg.decode(),
            Event::Pagefault {
                flags: UFFD_PAGEFAULT_FLAG_WRITE,
                address: 0x7f00_0000_1234
            }
        );
        msg.event = UFFD_EVENT_REMOVE;
        msg.arg = [0x1000, 0x3000, 0];
        assert_eq!(
            msg.decode(),
            Event::Remove {
                start: 0x1000,
                end: 0x3000
            }
        );
        msg.event = UFFD_EVENT_FORK;
        assert_eq!(msg.decode(), Event::Other(UFFD_EVENT_FORK));
    }
}
