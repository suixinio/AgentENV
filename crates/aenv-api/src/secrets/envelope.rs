//! The envelope `[secrets.pg]` stores values in: AES-256-GCM under one
//! master key that lives in a file, never in the database holding the
//! ciphertext.

use std::path::Path;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use anyhow::{bail, Context, Result};
use base64::Engine;
use zeroize::Zeroizing;

pub const KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;

/// One sealed value: the ciphertext and the nonce it was sealed under, both
/// stored in the same row.
pub struct Sealed {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

/// Everything a value is bound to besides its bytes, all of it read back
/// from the row the ciphertext sits in. A row whose name, version, kind or
/// pin was altered no longer opens, which is what keeps write access to the
/// table from being the ability to substitute, re-type or re-scope a secret.
pub struct Binding<'a> {
    pub name: &'a str,
    pub version: i64,
    pub kind: &'a str,
    pub allowed_hosts: &'a [String],
}

impl Binding<'_> {
    /// The pin is sorted so the same set of hosts binds the same way in
    /// whatever order the row hands it back; none of the fields can contain
    /// the separators, so the encoding is unambiguous.
    fn aad(&self) -> String {
        let mut hosts: Vec<&str> = self.allowed_hosts.iter().map(String::as_str).collect();
        hosts.sort_unstable();
        format!(
            "{}:{}:{}:{}",
            self.name,
            self.version,
            self.kind,
            hosts.join(",")
        )
    }
}

/// Seals and opens secret values. Holds the master key and nothing else, so
/// the set of callers that can decrypt is the set that holds one of these.
pub struct Envelope {
    cipher: Aes256Gcm,
}

impl Envelope {
    /// Reads a base64 32-byte key. Surrounding whitespace is stripped, so a
    /// file written with a trailing newline works.
    pub fn from_key_file(path: &Path) -> Result<Self> {
        let encoded = Zeroizing::new(
            std::fs::read_to_string(path)
                .with_context(|| format!("read the secrets master key from {path:?}"))?,
        );
        let key = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .with_context(|| format!("the secrets master key in {path:?} is not base64"))?,
        );
        Self::from_key_bytes(&key)
            .with_context(|| format!("the secrets master key in {path:?} is unusable"))
    }

    pub fn from_key_bytes(key: &[u8]) -> Result<Self> {
        if key.len() != KEY_BYTES {
            bail!(
                "the secrets master key must be {KEY_BYTES} bytes, this one is {}",
                key.len()
            );
        }
        Ok(Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
        })
    }

    pub fn seal(&self, binding: &Binding<'_>, plaintext: &[u8]) -> Result<Sealed> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let aad = binding.aad();
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: aad.as_bytes(),
                },
            )
            // The error carries no plaintext, but neither does it carry
            // anything an operator can act on; the context does.
            .map_err(|_| anyhow::anyhow!("failed to seal a secret value"))?;
        Ok(Sealed {
            ciphertext,
            nonce: nonce.to_vec(),
        })
    }

    pub fn open(&self, binding: &Binding<'_>, sealed: &Sealed) -> Result<Zeroizing<Vec<u8>>> {
        if sealed.nonce.len() != NONCE_BYTES {
            bail!(
                "a stored nonce is {} bytes, not {NONCE_BYTES}",
                sealed.nonce.len()
            );
        }
        let aad = binding.aad();
        let plaintext = self
            .cipher
            .decrypt(
                Nonce::from_slice(&sealed.nonce),
                Payload {
                    msg: &sealed.ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to open the stored value of {:?} version {}: wrong master key, or a \
                     row that was moved or altered",
                    binding.name,
                    binding.version
                )
            })?;
        Ok(Zeroizing::new(plaintext))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> Envelope {
        Envelope::from_key_bytes(&[7u8; KEY_BYTES]).unwrap()
    }

    fn opaque(name: &str, version: i64) -> Binding<'_> {
        Binding {
            name,
            version,
            kind: "opaque",
            allowed_hosts: &[],
        }
    }

    #[test]
    fn a_sealed_value_opens_under_the_binding_it_was_sealed_for() {
        let envelope = envelope();
        let sealed = envelope
            .seal(&opaque("tenant_db", 3), b"postgres://u:p@h/db")
            .unwrap();
        assert_eq!(
            envelope
                .open(&opaque("tenant_db", 3), &sealed)
                .unwrap()
                .as_slice(),
            b"postgres://u:p@h/db"
        );
        assert!(!sealed.ciphertext.windows(8).any(|w| w == b"postgres"));
    }

    #[test]
    fn a_row_moved_to_another_name_or_version_does_not_open() {
        let envelope = envelope();
        let sealed = envelope
            .seal(&opaque("tenant_db_ws42", 3), b"secret")
            .unwrap();
        assert!(envelope
            .open(&opaque("tenant_db_ws99", 3), &sealed)
            .is_err());
        assert!(envelope
            .open(&opaque("tenant_db_ws42", 4), &sealed)
            .is_err());
        assert!(envelope
            .open(&opaque("tenant_db_ws42", 2), &sealed)
            .is_err());
    }

    #[test]
    fn a_row_whose_kind_or_pin_was_altered_does_not_open() {
        let envelope = envelope();
        let pinned = ["api.openai.com".to_string(), "*.github.com".to_string()];
        let sealed = envelope
            .seal(
                &Binding {
                    name: "openai",
                    version: 1,
                    kind: "opaque",
                    allowed_hosts: &pinned,
                },
                b"sk-live",
            )
            .unwrap();

        let reordered = ["*.github.com".to_string(), "api.openai.com".to_string()];
        assert!(
            envelope
                .open(
                    &Binding {
                        name: "openai",
                        version: 1,
                        kind: "opaque",
                        allowed_hosts: &reordered,
                    },
                    &sealed,
                )
                .is_ok(),
            "the order a row hands the pin back in is not part of the binding"
        );
        for (kind, hosts) in [
            ("fields", &pinned[..]),
            ("opaque", &[][..]),
            ("opaque", &pinned[..1]),
            (
                "opaque",
                &["api.openai.com".to_string(), "evil.example".to_string()][..],
            ),
        ] {
            assert!(
                envelope
                    .open(
                        &Binding {
                            name: "openai",
                            version: 1,
                            kind,
                            allowed_hosts: hosts,
                        },
                        &sealed,
                    )
                    .is_err(),
                "kind {kind:?} with pin {hosts:?} must not open"
            );
        }
    }

    #[test]
    fn another_key_does_not_open_it() {
        let sealed = envelope().seal(&opaque("db", 1), b"secret").unwrap();
        let other = Envelope::from_key_bytes(&[9u8; KEY_BYTES]).unwrap();
        assert!(other.open(&opaque("db", 1), &sealed).is_err());
    }

    #[test]
    fn two_seals_of_one_value_reuse_no_nonce() {
        let envelope = envelope();
        let first = envelope.seal(&opaque("db", 1), b"secret").unwrap();
        let second = envelope.seal(&opaque("db", 1), b"secret").unwrap();
        assert_ne!(first.nonce, second.nonce);
        assert_ne!(first.ciphertext, second.ciphertext);
        assert_eq!(first.nonce.len(), NONCE_BYTES);
    }

    #[test]
    fn an_altered_ciphertext_or_nonce_does_not_open() {
        let envelope = envelope();
        let sealed = envelope.seal(&opaque("db", 1), b"secret").unwrap();

        let mut flipped = Sealed {
            ciphertext: sealed.ciphertext.clone(),
            nonce: sealed.nonce.clone(),
        };
        flipped.ciphertext[0] ^= 1;
        assert!(envelope.open(&opaque("db", 1), &flipped).is_err());

        let mut renonced = Sealed {
            ciphertext: sealed.ciphertext.clone(),
            nonce: sealed.nonce.clone(),
        };
        renonced.nonce[0] ^= 1;
        assert!(envelope.open(&opaque("db", 1), &renonced).is_err());

        let truncated = Sealed {
            ciphertext: sealed.ciphertext,
            nonce: vec![0u8; NONCE_BYTES - 1],
        };
        assert!(envelope.open(&opaque("db", 1), &truncated).is_err());
    }

    #[test]
    fn a_key_that_is_not_thirty_two_bytes_is_refused() {
        for len in [0, 16, 31, 33, 64] {
            assert!(Envelope::from_key_bytes(&vec![0u8; len]).is_err(), "{len}");
        }
    }

    #[test]
    fn the_key_file_is_base64_and_tolerates_a_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let encoded = base64::engine::general_purpose::STANDARD.encode([3u8; KEY_BYTES]);
        std::fs::write(&path, format!("{encoded}\n")).unwrap();
        let envelope = Envelope::from_key_file(&path).unwrap();
        let sealed = envelope.seal(&opaque("db", 1), b"v").unwrap();
        assert_eq!(
            envelope.open(&opaque("db", 1), &sealed).unwrap().as_slice(),
            b"v"
        );

        std::fs::write(&path, "not base64!!").unwrap();
        assert!(Envelope::from_key_file(&path).is_err());

        let short = base64::engine::general_purpose::STANDARD.encode([3u8; 16]);
        std::fs::write(&path, short).unwrap();
        assert!(Envelope::from_key_file(&path).is_err());
    }
}
