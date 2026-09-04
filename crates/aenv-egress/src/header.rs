use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// The only header version this crate emits or accepts.
pub const IDENTITY_HEADER_VERSION: u32 = 1;

pub const NONCE_LEN: usize = 16;

type HmacSha256 = Hmac<Sha256>;

/// The sandbox's egress policy as the broker must apply it to upstream
/// connections made on the sandbox's behalf.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicySummary {
    pub allow_internet: bool,
    #[serde(default)]
    pub allowed_cidrs: Vec<String>,
    #[serde(default)]
    pub denied_cidrs: Vec<String>,
}

/// Identity of one brokered connection, written by the runtime as the first
/// frame of every stream. `hmac` covers every other field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityHeader {
    pub v: u32,
    pub node_id: String,
    pub sandbox_id: String,
    pub execution_id: String,
    pub template_id: String,
    pub port: u16,
    pub handler: String,
    pub params: serde_json::Value,
    pub original_dst: Option<SocketAddr>,
    pub egress: EgressPolicySummary,
    pub guest_addr: Option<SocketAddr>,
    pub issued_at_unix_ms: u64,
    #[serde(with = "base64_nonce")]
    pub nonce: [u8; NONCE_LEN],
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hmac: String,
}

/// The broker's answer to an identity header.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Ack {
    pub fn accepted() -> Self {
        Self {
            accepted: true,
            reason: None,
        }
    }

    pub fn rejected(reason: impl Into<String>) -> Self {
        Self {
            accepted: false,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("unsupported identity header version")]
    UnsupportedVersion,
    #[error("identity header carries no mac")]
    MissingMac,
    #[error("identity header mac is not base64")]
    MalformedMac,
    #[error("identity header mac does not verify")]
    BadMac,
    #[error("identity header issued outside the accepted clock skew")]
    SkewExceeded,
}

impl VerifyError {
    /// Stable wire reason for an [`Ack`].
    pub fn reason(&self) -> &'static str {
        match self {
            Self::UnsupportedVersion => "unsupported_version",
            Self::MissingMac => "missing_mac",
            Self::MalformedMac => "malformed_mac",
            Self::BadMac => "bad_mac",
            Self::SkewExceeded => "skew_exceeded",
        }
    }
}

impl IdentityHeader {
    pub fn fresh_nonce() -> [u8; NONCE_LEN] {
        rand::random()
    }

    pub fn now_unix_ms() -> u64 {
        unix_ms(SystemTime::now())
    }

    /// The bytes the mac is computed over: this header serialized as JSON
    /// with `hmac` absent. Field order is the declaration order.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut unsigned = self.clone();
        unsigned.hmac.clear();
        serde_json::to_vec(&unsigned).expect("identity header serializes")
    }

    pub fn sign(&mut self, key: &[u8]) {
        let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
        mac.update(&self.canonical_bytes());
        self.hmac = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    }

    /// Accepts the header if its version is known, `issued_at_unix_ms` is
    /// within `max_skew` of `now`, and any of `keys` verifies the mac.
    /// Replay protection is [`ReplayCache`]'s job, checked after this.
    pub fn verify<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        max_skew: Duration,
        now: SystemTime,
    ) -> Result<(), VerifyError> {
        if self.v != IDENTITY_HEADER_VERSION {
            return Err(VerifyError::UnsupportedVersion);
        }
        if self.hmac.is_empty() {
            return Err(VerifyError::MissingMac);
        }
        let tag = base64::engine::general_purpose::STANDARD
            .decode(&self.hmac)
            .map_err(|_| VerifyError::MalformedMac)?;
        let now_ms = unix_ms(now);
        let skew_ms = u64::try_from(max_skew.as_millis()).unwrap_or(u64::MAX);
        if now_ms.abs_diff(self.issued_at_unix_ms) > skew_ms {
            return Err(VerifyError::SkewExceeded);
        }
        let canonical = self.canonical_bytes();
        let verified = keys.iter().any(|key| {
            let mut mac =
                HmacSha256::new_from_slice(key.as_ref()).expect("hmac accepts any key length");
            mac.update(&canonical);
            mac.verify_slice(&tag).is_ok()
        });
        if verified {
            Ok(())
        } else {
            Err(VerifyError::BadMac)
        }
    }
}

fn unix_ms(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerdict {
    Fresh,
    Replayed,
    /// The cache holds `capacity` live nonces; the connection is refused
    /// rather than risk forgetting one that is still inside the window.
    Full,
}

impl ReplayVerdict {
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Fresh => None,
            Self::Replayed => Some("replayed_nonce"),
            Self::Full => Some("replay_cache_full"),
        }
    }
}

/// Remembers every nonce accepted inside the skew window so a captured
/// header cannot open a second stream. Size it for the connections expected
/// within one window; entries leave in expiry order as their windows close.
pub struct ReplayCache {
    window_ms: u64,
    capacity: usize,
    seen: HashSet<[u8; NONCE_LEN]>,
    by_expiry: BTreeSet<(u64, [u8; NONCE_LEN])>,
}

impl ReplayCache {
    pub fn new(window: Duration, capacity: usize) -> Self {
        Self {
            window_ms: u64::try_from(window.as_millis()).unwrap_or(u64::MAX),
            capacity,
            seen: HashSet::new(),
            by_expiry: BTreeSet::new(),
        }
    }

    pub fn check_and_insert(
        &mut self,
        nonce: [u8; NONCE_LEN],
        issued_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> ReplayVerdict {
        self.expire(now_unix_ms);
        if self.seen.contains(&nonce) {
            return ReplayVerdict::Replayed;
        }
        if self.seen.len() >= self.capacity {
            return ReplayVerdict::Full;
        }
        let expires_at = issued_at_unix_ms.saturating_add(self.window_ms);
        self.seen.insert(nonce);
        self.by_expiry.insert((expires_at, nonce));
        ReplayVerdict::Fresh
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    fn expire(&mut self, now_unix_ms: u64) {
        while let Some(&(expires_at, nonce)) = self.by_expiry.first() {
            if expires_at >= now_unix_ms {
                break;
            }
            self.by_expiry.pop_first();
            self.seen.remove(&nonce);
        }
    }
}

mod base64_nonce {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::NONCE_LEN;

    pub fn serialize<S: Serializer>(nonce: &[u8; NONCE_LEN], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(nonce))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; NONCE_LEN], D::Error> {
        let text = String::deserialize(d)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(serde::de::Error::custom)?;
        <[u8; NONCE_LEN]>::try_from(bytes)
            .map_err(|_| serde::de::Error::custom("nonce must be 16 bytes"))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub fn sample_header() -> IdentityHeader {
        IdentityHeader {
            v: IDENTITY_HEADER_VERSION,
            node_id: "node-a".into(),
            sandbox_id: "sbx-1".into(),
            execution_id: "exec-1".into(),
            template_id: "tmpl-1".into(),
            port: 40443,
            handler: "echo".into(),
            params: serde_json::json!({ "rules": { "api.example.com": [] } }),
            original_dst: Some("93.184.216.34:443".parse().unwrap()),
            egress: EgressPolicySummary {
                allow_internet: true,
                allowed_cidrs: vec![],
                denied_cidrs: vec!["203.0.113.0/24".into()],
            },
            guest_addr: Some("10.12.0.3:51000".parse().unwrap()),
            issued_at_unix_ms: 1_800_000_000_000,
            nonce: [7; NONCE_LEN],
            hmac: String::new(),
        }
    }

    pub fn at_ms(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{at_ms, sample_header};
    use super::*;

    const KEY: &[u8] = b"primary-key";
    const OTHER_KEY: &[u8] = b"rotated-key";
    const SKEW: Duration = Duration::from_secs(30);

    fn signed() -> IdentityHeader {
        let mut header = sample_header();
        header.sign(KEY);
        header
    }

    #[test]
    fn a_signed_header_verifies_and_roundtrips_through_json() {
        let header = signed();
        let json = serde_json::to_vec(&header).unwrap();
        let decoded: IdentityHeader = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(
            decoded.verify(&[KEY], SKEW, at_ms(header.issued_at_unix_ms)),
            Ok(())
        );
    }

    #[test]
    fn changing_any_covered_field_breaks_the_mac() {
        let now = at_ms(sample_header().issued_at_unix_ms);
        type Mutation = Box<dyn Fn(&mut IdentityHeader)>;
        let mutations: Vec<(&str, Mutation)> = vec![
            ("node_id", Box::new(|h| h.node_id.push('x'))),
            ("sandbox_id", Box::new(|h| h.sandbox_id = "sbx-2".into())),
            (
                "execution_id",
                Box::new(|h| h.execution_id = "exec-2".into()),
            ),
            ("template_id", Box::new(|h| h.template_id.push('x'))),
            ("port", Box::new(|h| h.port += 1)),
            ("handler", Box::new(|h| h.handler = "http".into())),
            (
                "params",
                Box::new(|h| h.params = serde_json::json!({ "rules": {} })),
            ),
            ("original_dst", Box::new(|h| h.original_dst = None)),
            (
                "egress.allow_internet",
                Box::new(|h| h.egress.allow_internet = false),
            ),
            (
                "egress.denied_cidrs",
                Box::new(|h| h.egress.denied_cidrs.clear()),
            ),
            ("guest_addr", Box::new(|h| h.guest_addr = None)),
            ("issued_at", Box::new(|h| h.issued_at_unix_ms += 1)),
            ("nonce", Box::new(|h| h.nonce[0] ^= 1)),
        ];
        for (name, mutate) in mutations {
            let mut header = signed();
            mutate(&mut header);
            assert_eq!(
                header.verify(&[KEY], SKEW, now),
                Err(VerifyError::BadMac),
                "mutating {name} must fail verification"
            );
        }
    }

    #[test]
    fn a_header_outside_the_skew_window_is_rejected_in_both_directions() {
        let header = signed();
        let issued = header.issued_at_unix_ms;
        assert_eq!(header.verify(&[KEY], SKEW, at_ms(issued + 30_000)), Ok(()));
        assert_eq!(header.verify(&[KEY], SKEW, at_ms(issued - 30_000)), Ok(()));
        assert_eq!(
            header.verify(&[KEY], SKEW, at_ms(issued + 30_001)),
            Err(VerifyError::SkewExceeded)
        );
        assert_eq!(
            header.verify(&[KEY], SKEW, at_ms(issued - 30_001)),
            Err(VerifyError::SkewExceeded)
        );
    }

    #[test]
    fn either_of_two_keys_verifies_and_a_third_does_not() {
        let now = at_ms(sample_header().issued_at_unix_ms);
        let with_primary = signed();
        let mut with_rotated = sample_header();
        with_rotated.sign(OTHER_KEY);

        let keys = [KEY, OTHER_KEY];
        assert_eq!(with_primary.verify(&keys, SKEW, now), Ok(()));
        assert_eq!(with_rotated.verify(&keys, SKEW, now), Ok(()));
        assert_eq!(
            with_primary.verify(&[b"unrelated".as_slice()], SKEW, now),
            Err(VerifyError::BadMac)
        );
    }

    #[test]
    fn unsigned_malformed_and_unknown_version_headers_are_rejected() {
        let now = at_ms(sample_header().issued_at_unix_ms);
        assert_eq!(
            sample_header().verify(&[KEY], SKEW, now),
            Err(VerifyError::MissingMac)
        );

        let mut malformed = signed();
        malformed.hmac = "not base64!".into();
        assert_eq!(
            malformed.verify(&[KEY], SKEW, now),
            Err(VerifyError::MalformedMac)
        );

        let mut future_version = sample_header();
        future_version.v = 2;
        future_version.sign(KEY);
        assert_eq!(
            future_version.verify(&[KEY], SKEW, now),
            Err(VerifyError::UnsupportedVersion)
        );
    }

    #[test]
    fn the_replay_cache_rejects_a_second_use_inside_the_window() {
        let mut cache = ReplayCache::new(SKEW, 16);
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 1_000),
            ReplayVerdict::Fresh
        );
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 1_000),
            ReplayVerdict::Replayed
        );
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 31_000),
            ReplayVerdict::Replayed
        );
        assert_eq!(
            cache.check_and_insert([2; 16], 1_000, 1_000),
            ReplayVerdict::Fresh
        );
    }

    #[test]
    fn the_replay_cache_forgets_a_nonce_once_its_window_closes() {
        let mut cache = ReplayCache::new(SKEW, 16);
        cache.check_and_insert([1; 16], 1_000, 1_000);
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 31_001),
            ReplayVerdict::Fresh
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_full_replay_cache_refuses_rather_than_evicting_a_live_nonce() {
        let mut cache = ReplayCache::new(SKEW, 2);
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 1_000),
            ReplayVerdict::Fresh
        );
        assert_eq!(
            cache.check_and_insert([2; 16], 1_000, 1_000),
            ReplayVerdict::Fresh
        );
        assert_eq!(
            cache.check_and_insert([3; 16], 1_000, 1_000),
            ReplayVerdict::Full
        );
        assert_eq!(
            cache.check_and_insert([1; 16], 1_000, 1_000),
            ReplayVerdict::Replayed
        );
        assert_eq!(
            cache.check_and_insert([3; 16], 40_000, 40_000),
            ReplayVerdict::Fresh
        );
    }

    #[test]
    fn a_nonce_dated_ahead_of_the_clock_does_not_keep_expired_nonces_in_the_cache() {
        let mut cache = ReplayCache::new(SKEW, 2);
        assert_eq!(
            cache.check_and_insert([1; 16], 26_000, 1_000),
            ReplayVerdict::Fresh
        );
        assert_eq!(
            cache.check_and_insert([2; 16], 1_000, 1_000),
            ReplayVerdict::Fresh
        );
        assert_eq!(
            cache.check_and_insert([3; 16], 31_001, 31_001),
            ReplayVerdict::Fresh
        );
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.check_and_insert([1; 16], 26_000, 31_001),
            ReplayVerdict::Replayed
        );
        assert_eq!(
            cache.check_and_insert([2; 16], 31_001, 31_001),
            ReplayVerdict::Full
        );
    }
}
