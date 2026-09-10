//! The one-message handshake Firecracker sends to the handler socket after
//! `PUT /snapshot/load` with a `Uffd` memory backend: a JSON array of region
//! mappings in the body and the userfaultfd (plus, on builds with
//! `use_memfd`, the memfd) as `SCM_RIGHTS`.

use std::io::{IoSlice, IoSliceMut};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

use anyhow::{bail, Context, Result};
use nix::sys::socket::{recvmsg, sendmsg, ControlMessage, ControlMessageOwned, MsgFlags, UnixAddr};
use serde::{Deserialize, Serialize};

/// Firecracker's `GuestRegionUffdMapping`. `page_size_kib` is the field's
/// name in releases before 1.13 and, despite the name, holds bytes; newer
/// releases send both. `guest_phys_addr` arrives from the 1.15.1 patch line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestRegionUffdMapping {
    pub base_host_virt_addr: u64,
    pub size: u64,
    pub offset: u64,
    #[serde(default)]
    pub page_size: u64,
    #[serde(default)]
    pub page_size_kib: u64,
    #[serde(default)]
    pub guest_phys_addr: u64,
}

impl GuestRegionUffdMapping {
    pub const DEFAULT_PAGE_SIZE: u64 = 4096;

    pub fn page_size(&self) -> u64 {
        if self.page_size != 0 {
            self.page_size
        } else if self.page_size_kib != 0 {
            self.page_size_kib
        } else {
            Self::DEFAULT_PAGE_SIZE
        }
    }

    pub fn contains(&self, host_addr: u64) -> bool {
        host_addr >= self.base_host_virt_addr && host_addr < self.base_host_virt_addr + self.size
    }

    /// Byte offset in the memory image of the page holding `host_addr`.
    pub fn image_offset(&self, host_addr: u64) -> u64 {
        self.offset + (host_addr - self.base_host_virt_addr)
    }

    pub fn host_addr(&self, image_offset: u64) -> u64 {
        self.base_host_virt_addr + (image_offset - self.offset)
    }

    pub fn end_offset(&self) -> u64 {
        self.offset + self.size
    }
}

/// What the handshake delivered.
#[derive(Debug)]
pub struct Handshake {
    /// The userfaultfd first; a second descriptor is the memfd when the
    /// Firecracker build shares it.
    pub fds: Vec<OwnedFd>,
    pub mappings: Vec<GuestRegionUffdMapping>,
}

const MAX_MAPPINGS_BYTES: usize = 1 << 20;
const MAX_FDS: usize = 4;

/// Receives the handshake on an accepted connection. Blocks until the message
/// arrives; the caller bounds that with a read timeout on the stream.
pub fn recv_handshake(stream: &UnixStream) -> Result<Handshake> {
    let mut body = vec![0u8; MAX_MAPPINGS_BYTES];
    let mut iov = [IoSliceMut::new(&mut body)];
    let mut cmsg = nix::cmsg_space!([std::os::fd::RawFd; MAX_FDS]);
    let msg = recvmsg::<UnixAddr>(
        stream.as_raw_fd(),
        &mut iov,
        Some(&mut cmsg),
        MsgFlags::empty(),
    )
    .context("recvmsg on the uffd handshake socket")?;
    if msg.flags.contains(MsgFlags::MSG_CTRUNC) {
        bail!("uffd handshake carried more descriptors than expected");
    }
    let n = msg.bytes;
    let mut fds = Vec::new();
    for cm in msg.cmsgs().context("parse handshake control messages")? {
        if let ControlMessageOwned::ScmRights(raw) = cm {
            for fd in raw {
                // SAFETY: SCM_RIGHTS descriptors are freshly installed in this
                // process and owned by nobody else.
                fds.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        }
    }
    if fds.is_empty() {
        bail!("uffd handshake carried no descriptor");
    }
    if n == 0 {
        bail!("uffd handshake carried no mapping data");
    }
    let mappings: Vec<GuestRegionUffdMapping> = serde_json::from_slice(&body[..n])
        .with_context(|| format!("decode uffd region mappings ({n} bytes)"))?;
    if mappings.is_empty() {
        bail!("uffd handshake carried an empty mapping set");
    }
    Ok(Handshake { fds, mappings })
}

/// Sends a handshake the way Firecracker does; used by tests and the
/// self-test, which act as the VMM.
pub fn send_handshake(
    stream: &UnixStream,
    mappings: &[GuestRegionUffdMapping],
    fds: &[std::os::fd::RawFd],
) -> Result<()> {
    let body = serde_json::to_vec(mappings).context("encode uffd region mappings")?;
    let iov = [IoSlice::new(&body)];
    let cmsg = [ControlMessage::ScmRights(fds)];
    let sent = sendmsg::<UnixAddr>(stream.as_raw_fd(), &iov, &cmsg, MsgFlags::empty(), None)
        .context("sendmsg on the uffd handshake socket")?;
    if sent != body.len() {
        bail!("uffd handshake sent {sent} of {} bytes", body.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_falls_back_across_field_generations() {
        let mut m = GuestRegionUffdMapping {
            base_host_virt_addr: 0x1000,
            size: 0x4000,
            offset: 0x8000,
            page_size: 0,
            page_size_kib: 0,
            guest_phys_addr: 0,
        };
        assert_eq!(m.page_size(), 4096);
        m.page_size_kib = 2 << 20;
        assert_eq!(m.page_size(), 2 << 20);
        m.page_size = 4096;
        assert_eq!(m.page_size(), 4096);
        assert!(m.contains(0x4fff));
        assert!(!m.contains(0x5000));
        assert_eq!(m.image_offset(0x2000), 0x9000);
        assert_eq!(m.host_addr(0x9000), 0x2000);
    }

    #[test]
    fn firecracker_json_with_extra_fields_decodes() {
        let json = r#"[{"base_host_virt_addr":140000000000000,"guest_phys_addr":0,"size":1073741824,"offset":0,"page_size":4096,"page_size_kib":4096}]"#;
        let m: Vec<GuestRegionUffdMapping> = serde_json::from_str(json).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].size, 1 << 30);
        assert_eq!(m[0].page_size(), 4096);
    }
}
