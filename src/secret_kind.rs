//! The shape half of a secret, which both a network policy and the grant
//! issuer read. The store, the service and the values live in `aenv-api`.

use std::fmt;

/// The shape of a stored secret. A secret keeps the shape it was created
/// with: a header substitution reads `Opaque` and a protocol handler reads
/// `Fields`, and a policy is checked against the stored shape before the
/// sandbox is placed rather than at connection time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKind {
    Opaque,
    Fields,
}

impl SecretKind {
    /// The spelling a store records alongside the ciphertext.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Opaque => "opaque",
            Self::Fields => "fields",
        }
    }

    pub fn parse(kind: &str) -> Option<Self> {
        match kind {
            "opaque" => Some(Self::Opaque),
            "fields" => Some(Self::Fields),
            _ => None,
        }
    }
}

impl fmt::Display for SecretKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stored_spelling_round_trips() {
        for kind in [SecretKind::Opaque, SecretKind::Fields] {
            assert_eq!(SecretKind::parse(kind.as_str()), Some(kind));
            assert_eq!(kind.to_string(), kind.as_str());
        }
        assert_eq!(SecretKind::parse("neither"), None);
    }
}
