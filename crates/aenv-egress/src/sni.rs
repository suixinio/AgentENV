//! Reads the server name out of a TLS ClientHello without a TLS stack, so the
//! broker can decide whether to terminate a connection before it does.

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SniError {
    /// More bytes are needed before the ClientHello can be judged.
    #[error("the client hello is not complete yet")]
    Incomplete,
    /// The bytes do not start a TLS handshake record.
    #[error("the stream does not start with a TLS handshake record")]
    NotTls,
    #[error("the client hello is malformed")]
    Malformed,
}

const RECORD_HANDSHAKE: u8 = 0x16;
const HANDSHAKE_CLIENT_HELLO: u8 = 0x01;
const EXTENSION_SERVER_NAME: u16 = 0;
const NAME_TYPE_HOST: u8 = 0;

/// Largest prefix worth buffering before giving up on finding a ClientHello.
pub const MAX_CLIENT_HELLO_BYTES: usize = 16 * 1024;

/// `Ok(Some(name))` for a complete ClientHello with a host name, `Ok(None)`
/// for a complete one without. Names are lowercased; a trailing dot is
/// dropped.
pub fn parse_sni(buf: &[u8]) -> Result<Option<String>, SniError> {
    let handshake = reassemble_handshake(buf)?;
    let mut r = Reader::new(&handshake);
    if r.u8()? != HANDSHAKE_CLIENT_HELLO {
        return Err(SniError::NotTls);
    }
    let body_len = r.u24()? as usize;
    if r.remaining() < body_len {
        return Err(SniError::Incomplete);
    }
    let mut body = Reader::new(r.take(body_len)?);
    body.skip(2)?; // client version
    body.skip(32)?; // random
    let session_len = body.u8()? as usize;
    body.skip(session_len)?;
    let ciphers_len = body.u16()? as usize;
    body.skip(ciphers_len)?;
    let compression_len = body.u8()? as usize;
    body.skip(compression_len)?;
    if body.remaining() == 0 {
        return Ok(None);
    }
    let extensions_len = body.u16()? as usize;
    let mut extensions = Reader::new(body.take(extensions_len)?);
    while extensions.remaining() > 0 {
        let kind = extensions.u16()?;
        let len = extensions.u16()? as usize;
        let data = extensions.take(len)?;
        if kind != EXTENSION_SERVER_NAME {
            continue;
        }
        let mut names = Reader::new(data);
        let list_len = names.u16()? as usize;
        let mut list = Reader::new(names.take(list_len)?);
        while list.remaining() > 0 {
            let name_type = list.u8()?;
            let name_len = list.u16()? as usize;
            let name = list.take(name_len)?;
            if name_type == NAME_TYPE_HOST {
                let text = std::str::from_utf8(name).map_err(|_| SniError::Malformed)?;
                let text = text.trim_end_matches('.').to_ascii_lowercase();
                if text.is_empty()
                    || !text
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
                {
                    return Err(SniError::Malformed);
                }
                return Ok(Some(text));
            }
        }
        return Ok(None);
    }
    Ok(None)
}

// A ClientHello may span several handshake records; concatenate their
// payloads until the handshake message length is satisfied.
fn reassemble_handshake(buf: &[u8]) -> Result<Vec<u8>, SniError> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        if buf.len() < offset + 5 {
            if offset == 0 && !buf.is_empty() && buf[0] != RECORD_HANDSHAKE {
                return Err(SniError::NotTls);
            }
            return Err(SniError::Incomplete);
        }
        let record = &buf[offset..];
        if record[0] != RECORD_HANDSHAKE || record[1] != 0x03 {
            return if offset == 0 {
                Err(SniError::NotTls)
            } else {
                Err(SniError::Malformed)
            };
        }
        let len = u16::from_be_bytes([record[3], record[4]]) as usize;
        if record.len() < 5 + len {
            return Err(SniError::Incomplete);
        }
        out.extend_from_slice(&record[5..5 + len]);
        offset += 5 + len;
        if out.len() >= 4 {
            let need = 4 + u32::from_be_bytes([0, out[1], out[2], out[3]]) as usize;
            if out.len() >= need {
                return Ok(out);
            }
        }
        if offset >= buf.len() {
            return Err(SniError::Incomplete);
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], SniError> {
        if self.remaining() < n {
            return Err(SniError::Malformed);
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn skip(&mut self, n: usize) -> Result<(), SniError> {
        self.take(n).map(|_| ())
    }

    fn u8(&mut self) -> Result<u8, SniError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, SniError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u24(&mut self) -> Result<u32, SniError> {
        let b = self.take(3)?;
        Ok(u32::from_be_bytes([0, b[0], b[1], b[2]]))
    }
}

/// Builds a minimal ClientHello record, for tests and for probes.
pub fn client_hello_with_sni(name: Option<&str>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0u8; 32]);
    body.push(0); // session id
    body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]); // one cipher suite
    body.extend_from_slice(&[0x01, 0x00]); // null compression
    let mut extensions = Vec::new();
    if let Some(name) = name {
        let mut entry = Vec::new();
        entry.push(NAME_TYPE_HOST);
        entry.extend_from_slice(&(name.len() as u16).to_be_bytes());
        entry.extend_from_slice(name.as_bytes());
        let mut list = Vec::new();
        list.extend_from_slice(&(entry.len() as u16).to_be_bytes());
        list.extend_from_slice(&entry);
        extensions.extend_from_slice(&EXTENSION_SERVER_NAME.to_be_bytes());
        extensions.extend_from_slice(&(list.len() as u16).to_be_bytes());
        extensions.extend_from_slice(&list);
    }
    // An unrelated extension, so the parser has to skip one.
    extensions.extend_from_slice(&[0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04]);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);

    let mut handshake = vec![HANDSHAKE_CLIENT_HELLO];
    handshake.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    handshake.extend_from_slice(&body);

    let mut record = vec![RECORD_HANDSHAKE, 0x03, 0x01];
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_server_name_is_read_and_lowercased() {
        let hello = client_hello_with_sni(Some("API.Example.COM."));
        assert_eq!(parse_sni(&hello), Ok(Some("api.example.com".to_string())));
    }

    #[test]
    fn a_hello_without_sni_is_complete_and_nameless() {
        let hello = client_hello_with_sni(None);
        assert_eq!(parse_sni(&hello), Ok(None));
    }

    #[test]
    fn a_partial_hello_asks_for_more_and_non_tls_is_refused() {
        let hello = client_hello_with_sni(Some("a.example"));
        for cut in [0, 3, 5, 20, hello.len() - 1] {
            assert_eq!(
                parse_sni(&hello[..cut]),
                Err(SniError::Incomplete),
                "cut at {cut}"
            );
        }
        assert_eq!(parse_sni(b"GET / HTTP/1.1\r\n"), Err(SniError::NotTls));
        assert_eq!(
            parse_sni(b"\x16\x02\x00\x00\x05abcde"),
            Err(SniError::NotTls)
        );
    }

    #[test]
    fn a_hello_split_over_two_records_is_reassembled() {
        let hello = client_hello_with_sni(Some("split.example"));
        let payload = &hello[5..];
        let (a, b) = payload.split_at(payload.len() / 2);
        let mut split = vec![0x16, 0x03, 0x01];
        split.extend_from_slice(&(a.len() as u16).to_be_bytes());
        split.extend_from_slice(a);
        split.extend_from_slice(&[0x16, 0x03, 0x01]);
        split.extend_from_slice(&(b.len() as u16).to_be_bytes());
        split.extend_from_slice(b);
        assert_eq!(parse_sni(&split), Ok(Some("split.example".to_string())));
    }

    #[test]
    fn a_name_with_bad_bytes_is_malformed() {
        let hello = client_hello_with_sni(Some("bad name"));
        assert_eq!(parse_sni(&hello), Err(SniError::Malformed));
    }
}
