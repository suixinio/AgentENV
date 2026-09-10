//! The nbd transmission protocol: the fixed-size request and reply headers the
//! kernel driver exchanges with a userspace server over one socket.

use anyhow::{bail, Result};

pub const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
pub const NBD_REPLY_MAGIC: u32 = 0x6744_6698;

pub const REQUEST_LEN: usize = 28;
pub const REPLY_LEN: usize = 16;

pub const NBD_FLAG_HAS_FLAGS: u64 = 1 << 0;
pub const NBD_FLAG_READ_ONLY: u64 = 1 << 1;
pub const NBD_FLAG_SEND_FLUSH: u64 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u64 = 1 << 3;
pub const NBD_FLAG_SEND_TRIM: u64 = 1 << 5;
pub const NBD_FLAG_CAN_MULTI_CONN: u64 = 1 << 8;

pub const NBD_CFLAG_DESTROY_ON_DISCONNECT: u64 = 1 << 0;

/// Bit 0 of the request's 16-bit flags word. `linux/nbd.h` spells the same bit
/// as `1 << 16` of the combined 32-bit type word the two fields share.
pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NbdCommand {
    Read,
    Write,
    Disc,
    Flush,
    Trim,
}

impl NbdCommand {
    /// Returns `None` for a command this server does not implement.
    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            0 => Some(Self::Read),
            1 => Some(Self::Write),
            2 => Some(Self::Disc),
            3 => Some(Self::Flush),
            4 => Some(Self::Trim),
            _ => None,
        }
    }

    pub fn as_raw(self) -> u16 {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Disc => 2,
            Self::Flush => 3,
            Self::Trim => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NbdRequest {
    pub flags: u16,
    pub command: u16,
    pub handle: [u8; 8],
    pub from: u64,
    pub len: u32,
}

impl NbdRequest {
    pub fn decode(buf: &[u8; REQUEST_LEN]) -> Result<Self> {
        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        if magic != NBD_REQUEST_MAGIC {
            bail!("nbd request magic is {magic:#010x}, expected {NBD_REQUEST_MAGIC:#010x}");
        }
        Ok(Self {
            flags: u16::from_be_bytes(buf[4..6].try_into().unwrap()),
            command: u16::from_be_bytes(buf[6..8].try_into().unwrap()),
            handle: buf[8..16].try_into().unwrap(),
            from: u64::from_be_bytes(buf[16..24].try_into().unwrap()),
            len: u32::from_be_bytes(buf[24..28].try_into().unwrap()),
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_LEN] {
        let mut buf = [0u8; REQUEST_LEN];
        buf[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.flags.to_be_bytes());
        buf[6..8].copy_from_slice(&self.command.to_be_bytes());
        buf[8..16].copy_from_slice(&self.handle);
        buf[16..24].copy_from_slice(&self.from.to_be_bytes());
        buf[24..28].copy_from_slice(&self.len.to_be_bytes());
        buf
    }

    pub fn kind(&self) -> Option<NbdCommand> {
        NbdCommand::from_raw(self.command)
    }

    pub fn fua(&self) -> bool {
        self.flags & NBD_CMD_FLAG_FUA != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NbdReply {
    pub error: u32,
    pub handle: [u8; 8],
}

impl NbdReply {
    pub fn decode(buf: &[u8; REPLY_LEN]) -> Result<Self> {
        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        if magic != NBD_REPLY_MAGIC {
            bail!("nbd reply magic is {magic:#010x}, expected {NBD_REPLY_MAGIC:#010x}");
        }
        Ok(Self {
            error: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            handle: buf[8..16].try_into().unwrap(),
        })
    }

    pub fn encode(&self) -> [u8; REPLY_LEN] {
        let mut buf = [0u8; REPLY_LEN];
        buf[0..4].copy_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
        buf[4..8].copy_from_slice(&self.error.to_be_bytes());
        buf[8..16].copy_from_slice(&self.handle);
        buf
    }
}

/// Turns a target result (0, or a negative errno) into the reply's positive
/// error word. Anything unexpected becomes `EIO` rather than a zero success.
pub fn reply_error(result: i32) -> u32 {
    match result {
        0 => 0,
        negative if negative < 0 => negative.unsigned_abs(),
        _ => libc::EIO as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_survives_an_encode_decode_round_trip() {
        let request = NbdRequest {
            flags: NBD_CMD_FLAG_FUA,
            command: NbdCommand::Write.as_raw(),
            handle: [1, 2, 3, 4, 5, 6, 7, 8],
            from: 0x0102_0304_0506_0708,
            len: 0x0002_0000,
        };
        let encoded = request.encode();
        assert_eq!(encoded.len(), REQUEST_LEN);
        assert_eq!(NbdRequest::decode(&encoded).unwrap(), request);
    }

    #[test]
    fn a_request_encodes_every_field_in_network_byte_order() {
        let request = NbdRequest {
            flags: 0,
            command: NbdCommand::Read.as_raw(),
            handle: [0xAA; 8],
            from: 4096,
            len: 512,
        };
        let encoded = request.encode();
        assert_eq!(&encoded[0..4], &[0x25, 0x60, 0x95, 0x13]);
        assert_eq!(&encoded[4..8], &[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(&encoded[16..24], &4096u64.to_be_bytes());
        assert_eq!(&encoded[24..28], &512u32.to_be_bytes());
    }

    #[test]
    fn a_request_with_a_foreign_magic_is_refused() {
        let mut encoded = NbdRequest {
            flags: 0,
            command: 0,
            handle: [0; 8],
            from: 0,
            len: 0,
        }
        .encode();
        encoded[0] = 0x12;
        assert!(NbdRequest::decode(&encoded).is_err());
    }

    #[test]
    fn a_reply_survives_an_encode_decode_round_trip() {
        let reply = NbdReply {
            error: libc::EIO as u32,
            handle: [9, 8, 7, 6, 5, 4, 3, 2],
        };
        let encoded = reply.encode();
        assert_eq!(encoded.len(), REPLY_LEN);
        assert_eq!(&encoded[0..4], &[0x67, 0x44, 0x66, 0x98]);
        assert_eq!(NbdReply::decode(&encoded).unwrap(), reply);
    }

    #[test]
    fn the_fua_flag_rides_bit_zero_of_the_flags_word() {
        let request = NbdRequest {
            flags: NBD_CMD_FLAG_FUA,
            command: NbdCommand::Write.as_raw(),
            handle: [0; 8],
            from: 0,
            len: 4096,
        };
        let combined = u32::from_be_bytes(request.encode()[4..8].try_into().unwrap());
        assert_eq!(combined, (1 << 16) | u32::from(NbdCommand::Write.as_raw()));
        assert!(request.fua());
    }

    #[test]
    fn every_command_the_kernel_sends_maps_to_a_known_kind() {
        for (raw, expected) in [
            (0u16, NbdCommand::Read),
            (1, NbdCommand::Write),
            (2, NbdCommand::Disc),
            (3, NbdCommand::Flush),
            (4, NbdCommand::Trim),
        ] {
            assert_eq!(NbdCommand::from_raw(raw), Some(expected));
            assert_eq!(expected.as_raw(), raw);
        }
        assert_eq!(NbdCommand::from_raw(6), None);
    }

    #[test]
    fn a_target_result_becomes_a_positive_reply_error() {
        assert_eq!(reply_error(0), 0);
        assert_eq!(reply_error(-libc::EPERM), libc::EPERM as u32);
        assert_eq!(reply_error(-libc::EINVAL), libc::EINVAL as u32);
        assert_eq!(reply_error(4096), libc::EIO as u32);
    }
}
