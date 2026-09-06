use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The only header version this crate emits or accepts. The node and the
/// broker roll independently; this number is what decides which pairs talk,
/// and the broker is the half that rolls first.
pub const IDENTITY_HEADER_VERSION: u32 = 2;

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
/// frame of every stream. It is unauthenticated: the stream is a Unix socket
/// on this node, and the peer's uid is what the broker checks.
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

/// The wire reason a header of an unknown version is refused with.
pub const UNSUPPORTED_VERSION_REASON: &str = "unsupported header version";

impl IdentityHeader {
    pub fn now_unix_ms() -> u64 {
        unix_ms(SystemTime::now())
    }
}

fn unix_ms(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::sample_header;
    use super::*;

    #[test]
    fn a_header_roundtrips_through_json_and_carries_no_credential_field() {
        let header = sample_header();
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serde_json::from_str::<IdentityHeader>(&json).unwrap(),
            header
        );
        for gone in ["nonce", "hmac"] {
            assert!(!json.contains(gone), "{gone} is still on the wire: {json}");
        }
    }

    #[test]
    fn the_version_this_crate_emits_is_the_one_it_accepts() {
        assert_eq!(sample_header().v, IDENTITY_HEADER_VERSION);
        assert_eq!(IDENTITY_HEADER_VERSION, 2);
    }
}
