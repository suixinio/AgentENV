//! Generic netlink control for the kernel `nbd` family: the CONNECT,
//! DISCONNECT, RECONFIGURE and STATUS commands of `linux/nbd-netlink.h`.

use anyhow::{bail, Context, Result};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

const NETLINK_GENERIC: libc::c_int = 16;
const GENL_ID_CTRL: u16 = 16;
const CTRL_CMD_GETFAMILY: u8 = 3;
const CTRL_VERSION: u8 = 1;
const CTRL_ATTR_FAMILY_ID: u16 = 1;
const CTRL_ATTR_FAMILY_NAME: u16 = 2;

const NLMSG_NOOP: u16 = 1;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const NLM_F_REQUEST: u16 = 0x01;
const NLM_F_ACK: u16 = 0x04;

// Strict generic-netlink validation refuses an NLA_NESTED attribute that does
// not carry this bit, so every nest built here sets it.
const NLA_F_NESTED: u16 = 0x8000;
const NLA_TYPE_MASK: u16 = !(0x8000 | 0x4000);

const NLMSG_HDR_LEN: usize = 16;
const GENL_HDR_LEN: usize = 4;
const NLA_HDR_LEN: usize = 4;

const NBD_GENL_FAMILY_NAME: &str = "nbd";
const NBD_GENL_VERSION: u8 = 1;

const NBD_CMD_CONNECT: u8 = 1;
const NBD_CMD_DISCONNECT: u8 = 2;
const NBD_CMD_RECONFIGURE: u8 = 3;
const NBD_CMD_STATUS: u8 = 5;

const NBD_ATTR_INDEX: u16 = 1;
const NBD_ATTR_SIZE_BYTES: u16 = 2;
const NBD_ATTR_BLOCK_SIZE_BYTES: u16 = 3;
const NBD_ATTR_TIMEOUT: u16 = 4;
const NBD_ATTR_SERVER_FLAGS: u16 = 5;
const NBD_ATTR_CLIENT_FLAGS: u16 = 6;
const NBD_ATTR_SOCKETS: u16 = 7;
const NBD_ATTR_DEAD_CONN_TIMEOUT: u16 = 8;
const NBD_ATTR_DEVICE_LIST: u16 = 9;
const NBD_ATTR_BACKEND_IDENTIFIER: u16 = 10;

const NBD_SOCK_ITEM: u16 = 1;
const NBD_SOCK_FD: u16 = 1;

const NBD_DEVICE_ITEM: u16 = 1;
const NBD_DEVICE_INDEX: u16 = 1;
const NBD_DEVICE_CONNECTED: u16 = 2;

const RECV_BUF_LEN: usize = 64 * 1024;
const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// What a device is configured with at CONNECT time. `index` left as `None`
/// lets the kernel allocate a free device and report it back.
#[derive(Debug, Default, Clone)]
pub struct ConnectSpec {
    pub index: Option<u32>,
    pub size_bytes: u64,
    pub block_size: u32,
    pub timeout: Option<Duration>,
    pub dead_conn_timeout: Option<Duration>,
    pub server_flags: u64,
    pub client_flags: u64,
    pub sockets: Vec<RawFd>,
    pub backend_identifier: Option<String>,
}

/// What RECONFIGURE changes on a device that is already connected. A device
/// that was connected with a backend identifier must be named by it again.
#[derive(Debug, Default, Clone)]
pub struct ReconfigureSpec {
    pub size_bytes: Option<u64>,
    pub timeout: Option<Duration>,
    pub dead_conn_timeout: Option<Duration>,
    pub client_flags: Option<u64>,
    pub sockets: Vec<RawFd>,
    pub backend_identifier: Option<String>,
}

/// A generic-netlink socket bound to the kernel's `nbd` family.
#[derive(Debug)]
pub struct NbdNetlink {
    fd: OwnedFd,
    family_id: u16,
    seq: AtomicU32,
}

impl NbdNetlink {
    /// Opens the socket and resolves the `nbd` family id. Fails when the `nbd`
    /// module is not loaded, because the family only exists with it.
    pub fn open() -> Result<Self> {
        let fd = open_socket().context("open netlink socket")?;
        let netlink = Self {
            fd,
            family_id: GENL_ID_CTRL,
            seq: AtomicU32::new(1),
        };
        let family_id = netlink
            .resolve_family(NBD_GENL_FAMILY_NAME)
            .with_context(|| format!("resolve the `{NBD_GENL_FAMILY_NAME}` netlink family"))?;
        Ok(Self {
            family_id,
            ..netlink
        })
    }

    pub fn family_id(&self) -> u16 {
        self.family_id
    }

    /// Connects the device and returns the index the kernel bound it to.
    pub fn connect(&self, spec: &ConnectSpec) -> Result<u32> {
        if spec.sockets.is_empty() {
            bail!("nbd connect needs at least one socket");
        }
        let seq = self.next_seq();
        let mut msg = NlMsg::new(self.family_id, NBD_CMD_CONNECT, NBD_GENL_VERSION, seq);
        if let Some(index) = spec.index {
            msg.put_u32(NBD_ATTR_INDEX, index);
        }
        msg.put_u64(NBD_ATTR_SIZE_BYTES, spec.size_bytes);
        msg.put_u64(NBD_ATTR_BLOCK_SIZE_BYTES, u64::from(spec.block_size));
        msg.put_u64(NBD_ATTR_SERVER_FLAGS, spec.server_flags);
        msg.put_u64(NBD_ATTR_CLIENT_FLAGS, spec.client_flags);
        if let Some(timeout) = spec.timeout {
            msg.put_u64(NBD_ATTR_TIMEOUT, timeout.as_secs());
        }
        if let Some(timeout) = spec.dead_conn_timeout {
            msg.put_u64(NBD_ATTR_DEAD_CONN_TIMEOUT, timeout.as_secs());
        }
        if let Some(backend) = &spec.backend_identifier {
            msg.put_nul_str(NBD_ATTR_BACKEND_IDENTIFIER, backend);
        }
        put_sockets(&mut msg, &spec.sockets);

        let attrs = self.request(msg.finish(), seq).context("nbd connect")?;
        attr_u32(&attrs, NBD_ATTR_INDEX).context("the nbd connect reply carried no device index")
    }

    pub fn disconnect(&self, index: u32) -> Result<()> {
        let seq = self.next_seq();
        let mut msg = NlMsg::new(self.family_id, NBD_CMD_DISCONNECT, NBD_GENL_VERSION, seq);
        msg.put_u32(NBD_ATTR_INDEX, index);
        self.request(msg.finish(), seq)
            .with_context(|| format!("nbd disconnect {index}"))?;
        Ok(())
    }

    pub fn reconfigure(&self, index: u32, spec: &ReconfigureSpec) -> Result<()> {
        let seq = self.next_seq();
        let mut msg = NlMsg::new(self.family_id, NBD_CMD_RECONFIGURE, NBD_GENL_VERSION, seq);
        msg.put_u32(NBD_ATTR_INDEX, index);
        if let Some(size) = spec.size_bytes {
            msg.put_u64(NBD_ATTR_SIZE_BYTES, size);
        }
        if let Some(timeout) = spec.timeout {
            msg.put_u64(NBD_ATTR_TIMEOUT, timeout.as_secs());
        }
        if let Some(timeout) = spec.dead_conn_timeout {
            msg.put_u64(NBD_ATTR_DEAD_CONN_TIMEOUT, timeout.as_secs());
        }
        if let Some(flags) = spec.client_flags {
            msg.put_u64(NBD_ATTR_CLIENT_FLAGS, flags);
        }
        if let Some(backend) = &spec.backend_identifier {
            msg.put_nul_str(NBD_ATTR_BACKEND_IDENTIFIER, backend);
        }
        if !spec.sockets.is_empty() {
            put_sockets(&mut msg, &spec.sockets);
        }
        self.request(msg.finish(), seq)
            .with_context(|| format!("nbd reconfigure {index}"))?;
        Ok(())
    }

    /// Whether the kernel still lists this index as a connected device. An
    /// index the kernel does not know is reported as not connected.
    pub fn status(&self, index: u32) -> Result<bool> {
        let seq = self.next_seq();
        let mut msg = NlMsg::new(self.family_id, NBD_CMD_STATUS, NBD_GENL_VERSION, seq);
        msg.put_u32(NBD_ATTR_INDEX, index);
        let attrs = self
            .request(msg.finish(), seq)
            .with_context(|| format!("nbd status {index}"))?;
        let Some(list) = attrs
            .iter()
            .find(|(ty, _)| *ty == NBD_ATTR_DEVICE_LIST)
            .map(|(_, value)| value)
        else {
            return Ok(false);
        };
        for (ty, item) in parse_attrs(list).context("parse the nbd device list")? {
            if ty != NBD_DEVICE_ITEM {
                continue;
            }
            let fields = parse_attrs(item).context("parse an nbd device list item")?;
            let listed = fields
                .iter()
                .find(|(ty, _)| *ty == NBD_DEVICE_INDEX)
                .and_then(|(_, value)| value.get(..4))
                .map(|value| u32::from_ne_bytes(value.try_into().unwrap()));
            if listed != Some(index) {
                continue;
            }
            let connected = fields
                .iter()
                .find(|(ty, _)| *ty == NBD_DEVICE_CONNECTED)
                .and_then(|(_, value)| value.first())
                .copied()
                .unwrap_or(0);
            return Ok(connected != 0);
        }
        Ok(false)
    }

    fn resolve_family(&self, name: &str) -> Result<u16> {
        let seq = self.next_seq();
        let mut msg = NlMsg::new(GENL_ID_CTRL, CTRL_CMD_GETFAMILY, CTRL_VERSION, seq);
        msg.put_nul_str(CTRL_ATTR_FAMILY_NAME, name);
        let attrs = self.request(msg.finish(), seq)?;
        let id = attrs
            .iter()
            .find(|(ty, _)| *ty == CTRL_ATTR_FAMILY_ID)
            .and_then(|(_, value)| value.get(..2))
            .map(|value| u16::from_ne_bytes(value.try_into().unwrap()));
        id.context("the netlink family reply carried no family id")
    }

    fn next_seq(&self) -> u32 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn request(&self, msg: Vec<u8>, seq: u32) -> Result<Vec<(u16, Vec<u8>)>> {
        let sent = unsafe {
            libc::send(
                self.fd.as_raw_fd(),
                msg.as_ptr().cast(),
                msg.len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if sent < 0 {
            return Err(std::io::Error::last_os_error()).context("send a netlink request");
        }
        self.recv_reply(seq)
    }

    fn recv_reply(&self, seq: u32) -> Result<Vec<(u16, Vec<u8>)>> {
        let mut attrs: Vec<(u16, Vec<u8>)> = Vec::new();
        let mut buf = vec![0u8; RECV_BUF_LEN];
        loop {
            let received =
                unsafe { libc::recv(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
            if received < 0 {
                return Err(std::io::Error::last_os_error())
                    .context("receive a netlink reply (the kernel never answered)");
            }
            let received = received as usize;
            let mut offset = 0usize;
            while offset + NLMSG_HDR_LEN <= received {
                let header = &buf[offset..offset + NLMSG_HDR_LEN];
                let len = u32::from_ne_bytes(header[0..4].try_into().unwrap()) as usize;
                let kind = u16::from_ne_bytes(header[4..6].try_into().unwrap());
                let msg_seq = u32::from_ne_bytes(header[8..12].try_into().unwrap());
                if len < NLMSG_HDR_LEN || offset + len > received {
                    bail!("netlink message at {offset} declares an invalid length {len}");
                }
                let payload = &buf[offset + NLMSG_HDR_LEN..offset + len];
                if msg_seq == seq {
                    match kind {
                        NLMSG_NOOP => {}
                        NLMSG_ERROR => {
                            let raw = payload
                                .get(..4)
                                .context("netlink error reply is truncated")?;
                            let errno = i32::from_ne_bytes(raw.try_into().unwrap());
                            if errno != 0 {
                                return Err(std::io::Error::from_raw_os_error(-errno).into());
                            }
                            return Ok(attrs);
                        }
                        NLMSG_DONE => return Ok(attrs),
                        _ => {
                            let body = payload
                                .get(GENL_HDR_LEN..)
                                .context("generic netlink reply is truncated")?;
                            for (ty, value) in parse_attrs(body)? {
                                attrs.push((ty, value.to_vec()));
                            }
                        }
                    }
                }
                offset += align4(len);
            }
        }
    }
}

fn open_socket() -> Result<OwnedFd> {
    let raw = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            NETLINK_GENERIC,
        )
    };
    if raw < 0 {
        return Err(std::io::Error::last_os_error()).context("open AF_NETLINK socket");
    }
    // SAFETY: `raw` is a fresh descriptor this call owns.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };

    let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
    addr.nl_family = libc::AF_NETLINK as u16;
    let bound = unsafe {
        libc::bind(
            fd.as_raw_fd(),
            std::ptr::addr_of!(addr).cast(),
            std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
        )
    };
    if bound < 0 {
        return Err(std::io::Error::last_os_error()).context("bind AF_NETLINK socket");
    }

    let timeout = libc::timeval {
        tv_sec: RECV_TIMEOUT.as_secs() as libc::time_t,
        tv_usec: 0,
    };
    let set = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            std::ptr::addr_of!(timeout).cast(),
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if set < 0 {
        return Err(std::io::Error::last_os_error()).context("set the netlink receive timeout");
    }
    Ok(fd)
}

fn put_sockets(msg: &mut NlMsg, sockets: &[RawFd]) {
    let list = msg.nest_start(NBD_ATTR_SOCKETS);
    for fd in sockets {
        let item = msg.nest_start(NBD_SOCK_ITEM);
        msg.put_u32(NBD_SOCK_FD, *fd as u32);
        msg.nest_end(item);
    }
    msg.nest_end(list);
}

fn attr_u32(attrs: &[(u16, Vec<u8>)], ty: u16) -> Option<u32> {
    attrs
        .iter()
        .find(|(attr, _)| *attr == ty)
        .and_then(|(_, value)| value.get(..4))
        .map(|value| u32::from_ne_bytes(value.try_into().unwrap()))
}

fn align4(len: usize) -> usize {
    (len + 3) & !3
}

// Attributes go out in host byte order; the kernel reads them back the same way.
struct NlMsg {
    buf: Vec<u8>,
}

impl NlMsg {
    fn new(family: u16, cmd: u8, version: u8, seq: u32) -> Self {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(&0u32.to_ne_bytes());
        buf.extend_from_slice(&family.to_ne_bytes());
        buf.extend_from_slice(&(NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
        buf.extend_from_slice(&seq.to_ne_bytes());
        buf.extend_from_slice(&0u32.to_ne_bytes());
        buf.push(cmd);
        buf.push(version);
        buf.extend_from_slice(&0u16.to_ne_bytes());
        Self { buf }
    }

    fn put(&mut self, ty: u16, payload: &[u8]) {
        let len = NLA_HDR_LEN + payload.len();
        self.buf.extend_from_slice(&(len as u16).to_ne_bytes());
        self.buf.extend_from_slice(&ty.to_ne_bytes());
        self.buf.extend_from_slice(payload);
        while !self.buf.len().is_multiple_of(4) {
            self.buf.push(0);
        }
    }

    fn put_u32(&mut self, ty: u16, value: u32) {
        self.put(ty, &value.to_ne_bytes());
    }

    fn put_u64(&mut self, ty: u16, value: u64) {
        self.put(ty, &value.to_ne_bytes());
    }

    fn put_nul_str(&mut self, ty: u16, value: &str) {
        let mut payload = value.as_bytes().to_vec();
        payload.push(0);
        self.put(ty, &payload);
    }

    fn nest_start(&mut self, ty: u16) -> usize {
        let at = self.buf.len();
        self.buf.extend_from_slice(&0u16.to_ne_bytes());
        self.buf
            .extend_from_slice(&(ty | NLA_F_NESTED).to_ne_bytes());
        at
    }

    fn nest_end(&mut self, at: usize) {
        let len = (self.buf.len() - at) as u16;
        self.buf[at..at + 2].copy_from_slice(&len.to_ne_bytes());
    }

    fn finish(mut self) -> Vec<u8> {
        let len = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
        self.buf
    }
}

fn parse_attrs(buf: &[u8]) -> Result<Vec<(u16, &[u8])>> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset + NLA_HDR_LEN <= buf.len() {
        let len = u16::from_ne_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
        let ty =
            u16::from_ne_bytes(buf[offset + 2..offset + 4].try_into().unwrap()) & NLA_TYPE_MASK;
        if len < NLA_HDR_LEN || offset + len > buf.len() {
            bail!("netlink attribute at {offset} declares an invalid length {len}");
        }
        out.push((ty, &buf[offset + NLA_HDR_LEN..offset + len]));
        offset += align4(len);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(msg: &[u8]) -> &[u8] {
        &msg[NLMSG_HDR_LEN + GENL_HDR_LEN..]
    }

    #[test]
    fn a_message_declares_its_own_total_length_and_command() {
        let mut msg = NlMsg::new(0x2A, NBD_CMD_CONNECT, NBD_GENL_VERSION, 7);
        msg.put_u64(NBD_ATTR_SIZE_BYTES, 1 << 26);
        let encoded = msg.finish();
        assert_eq!(
            u32::from_ne_bytes(encoded[0..4].try_into().unwrap()) as usize,
            encoded.len()
        );
        assert_eq!(u16::from_ne_bytes(encoded[4..6].try_into().unwrap()), 0x2A);
        assert_eq!(
            u16::from_ne_bytes(encoded[6..8].try_into().unwrap()),
            NLM_F_REQUEST | NLM_F_ACK
        );
        assert_eq!(u32::from_ne_bytes(encoded[8..12].try_into().unwrap()), 7);
        assert_eq!(encoded[NLMSG_HDR_LEN], NBD_CMD_CONNECT);
        assert_eq!(encoded[NLMSG_HDR_LEN + 1], NBD_GENL_VERSION);
    }

    #[test]
    fn an_odd_length_attribute_is_padded_to_the_next_four_byte_boundary() {
        let mut msg = NlMsg::new(1, 1, 1, 1);
        msg.put_nul_str(NBD_ATTR_BACKEND_IDENTIFIER, "abc");
        msg.put_u32(NBD_ATTR_INDEX, 3);
        let encoded = msg.finish();
        let attrs = parse_attrs(body(&encoded)).unwrap();
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].0, NBD_ATTR_BACKEND_IDENTIFIER);
        assert_eq!(attrs[0].1, b"abc\0");
        assert_eq!(attrs[1].0, NBD_ATTR_INDEX);
        assert_eq!(u32::from_ne_bytes(attrs[1].1.try_into().unwrap()), 3);
    }

    #[test]
    fn a_socket_list_nests_one_item_per_descriptor_and_marks_the_nest() {
        let mut msg = NlMsg::new(1, NBD_CMD_CONNECT, NBD_GENL_VERSION, 1);
        put_sockets(&mut msg, &[11, 12, 13]);
        let encoded = msg.finish();
        let body = body(&encoded);
        let raw_type = u16::from_ne_bytes(body[2..4].try_into().unwrap());
        assert_eq!(
            raw_type & NLA_F_NESTED,
            NLA_F_NESTED,
            "strict validation refuses a nested attribute without NLA_F_NESTED"
        );

        let attrs = parse_attrs(body).unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].0, NBD_ATTR_SOCKETS);
        let items = parse_attrs(attrs[0].1).unwrap();
        assert_eq!(items.len(), 3);
        let mut fds = Vec::new();
        for (ty, item) in items {
            assert_eq!(ty, NBD_SOCK_ITEM);
            let fields = parse_attrs(item).unwrap();
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, NBD_SOCK_FD);
            fds.push(u32::from_ne_bytes(fields[0].1.try_into().unwrap()));
        }
        assert_eq!(fds, vec![11, 12, 13]);
    }

    #[test]
    fn a_nest_length_covers_its_header_and_every_item() {
        let mut msg = NlMsg::new(1, NBD_CMD_CONNECT, NBD_GENL_VERSION, 1);
        put_sockets(&mut msg, &[5]);
        let encoded = msg.finish();
        let body = body(&encoded);
        let declared = u16::from_ne_bytes(body[0..2].try_into().unwrap()) as usize;
        assert_eq!(declared, body.len());
        assert_eq!(declared, NLA_HDR_LEN + NLA_HDR_LEN + NLA_HDR_LEN + 4);
    }

    #[test]
    fn an_attribute_reaching_past_the_buffer_is_refused() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&64u16.to_ne_bytes());
        buf.extend_from_slice(&NBD_ATTR_INDEX.to_ne_bytes());
        buf.extend_from_slice(&0u32.to_ne_bytes());
        assert!(parse_attrs(&buf).is_err());
    }

    #[test]
    fn a_zero_length_attribute_header_is_refused() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u16.to_ne_bytes());
        buf.extend_from_slice(&NBD_ATTR_INDEX.to_ne_bytes());
        assert!(parse_attrs(&buf).is_err());
    }

    #[test]
    fn the_type_mask_strips_the_nested_and_byteorder_bits() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&8u16.to_ne_bytes());
        buf.extend_from_slice(&(NBD_ATTR_SOCKETS | NLA_F_NESTED).to_ne_bytes());
        buf.extend_from_slice(&0u32.to_ne_bytes());
        let attrs = parse_attrs(&buf).unwrap();
        assert_eq!(attrs[0].0, NBD_ATTR_SOCKETS);
    }
}
