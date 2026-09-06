//! The broker's audit trail: one line per brokered request, plus the events
//! that are not requests.
//!
//! Everything here is metadata. A header the broker injected is recorded by
//! **name**, never by value — the whole point of the arrangement is that the
//! value exists in one process, and an audit log that carried it would undo
//! that in the place it is hardest to notice. The same goes for the query
//! string, which is where credentials end up when an API takes them there.
//!
//! Lines go to `tracing` under the [`TARGET`] target, which the broker's
//! subscriber renders as JSON on stdout.

use std::sync::atomic::{AtomicU8, Ordering};

use crate::handler::ConnCtx;

/// The tracing target every line below carries, so a collector can route the
/// audit trail without parsing it.
pub const TARGET: &str = "egress.audit";

/// The longest path a line records. A path past this is truncated rather than
/// dropped: what it names still matters, and a request can make it arbitrarily
/// long.
pub const MAX_PATH_BYTES: usize = 1024;

/// What `[audit].level` admits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuditLevel {
    /// Every request and event, metadata only.
    #[default]
    Metadata,
    /// Nothing. Security events go too — a deployment that turns this off is
    /// saying it collects them elsewhere.
    None,
}

impl AuditLevel {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "metadata" => Some(Self::Metadata),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::None => "none",
        }
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(0);

/// Sets the process-wide level. Called once, before the listener binds.
pub fn set_level(level: AuditLevel) {
    LEVEL.store(
        match level {
            AuditLevel::Metadata => 0,
            AuditLevel::None => 1,
        },
        Ordering::Relaxed,
    );
}

pub fn level() -> AuditLevel {
    match LEVEL.load(Ordering::Relaxed) {
        1 => AuditLevel::None,
        _ => AuditLevel::Metadata,
    }
}

fn recording() -> bool {
    level() == AuditLevel::Metadata
}

/// The path a request named, without its query and bounded in length.
pub fn audited_path(uri: &http::Uri) -> String {
    let path = uri.path();
    if path.len() <= MAX_PATH_BYTES {
        return path.to_string();
    }
    let mut cut = MAX_PATH_BYTES;
    while cut > 0 && !path.is_char_boundary(cut) {
        cut -= 1;
    }
    path[..cut].to_string()
}

/// One brokered request, as the trail records it.
pub struct RequestRecord<'a> {
    pub ctx: &'a ConnCtx,
    pub node_id: &'a str,
    pub host: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub status: u16,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub latency_ms: u64,
    pub tls_version: &'a str,
    pub cipher: &'a str,
    pub upstream_addr: Option<std::net::SocketAddr>,
    /// The rule key that matched, which is not always the host: a wildcard
    /// rule matches many hosts and the trail has to say which rule ran.
    pub rule: &'a str,
    /// Header names the broker set. Never their values.
    pub injected_headers: &'a [String],
}

pub fn request(record: RequestRecord<'_>) {
    if !recording() {
        return;
    }
    let (dst_ip, dst_port) = match record.ctx.original_dst {
        Some(addr) => (addr.ip().to_string(), addr.port()),
        None => (String::new(), 0),
    };
    tracing::info!(
        target: TARGET,
        event = "request",
        ts = now_millis(),
        node_id = record.node_id,
        sandbox_id = %record.ctx.sandbox_id,
        execution_id = %record.ctx.execution_id,
        dst_ip,
        dst_port,
        scheme = "https",
        host = record.host,
        method = record.method,
        path = record.path,
        status = record.status,
        bytes_in = record.bytes_in,
        bytes_out = record.bytes_out,
        latency_ms = record.latency_ms,
        tls_version = record.tls_version,
        cipher = record.cipher,
        upstream_addr = record.upstream_addr.map(|addr| addr.to_string()).unwrap_or_default(),
        rule = record.rule,
        injected_headers = record.injected_headers.join(","),
    );
}

/// Something the broker refused, and why. A default-deny, a name that does
/// not match the one the rules chose, an upstream the policy rules out, or a
/// credential the rules asked for and the secret's own pin forbids.
pub fn security_event(ctx: &ConnCtx, node_id: &str, event: &str, host: &str, reason: &str) {
    if !recording() {
        return;
    }
    tracing::info!(
        target: TARGET,
        event = "security_event",
        ts = now_millis(),
        node_id,
        sandbox_id = %ctx.sandbox_id,
        execution_id = %ctx.execution_id,
        kind = event,
        host,
        reason,
    );
}

/// A TLS handshake that did not complete, on either side.
pub fn tls_handshake(ctx: &ConnCtx, node_id: &str, host: &str, side: &str, error: &str) {
    if !recording() {
        return;
    }
    tracing::info!(
        target: TARGET,
        event = "tls_handshake",
        ts = now_millis(),
        node_id,
        sandbox_id = %ctx.sandbox_id,
        execution_id = %ctx.execution_id,
        host,
        side,
        error,
    );
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_level_round_trips_through_its_configured_spelling() {
        for level in [AuditLevel::Metadata, AuditLevel::None] {
            assert_eq!(AuditLevel::parse(level.as_str()), Some(level));
        }
        assert_eq!(AuditLevel::parse("  metadata "), Some(AuditLevel::Metadata));
        assert_eq!(AuditLevel::parse("full"), None);
        assert_eq!(AuditLevel::parse(""), None);
    }

    #[test]
    fn a_path_is_recorded_without_its_query_and_bounded_in_length() {
        let uri: http::Uri = "https://api.test/v1/models?key=sk-live".parse().unwrap();
        assert_eq!(audited_path(&uri), "/v1/models");

        let long: http::Uri = format!("https://api.test/{}", "a".repeat(4096))
            .parse()
            .unwrap();
        assert_eq!(audited_path(&long).len(), MAX_PATH_BYTES);
    }

    /// Collects what a subscriber wrote, so a test can read the line back.
    #[cfg(feature = "bin")]
    #[derive(Clone, Default)]
    struct Collector(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    #[cfg(feature = "bin")]
    impl std::io::Write for Collector {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "bin")]
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Collector {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[cfg(feature = "bin")]
    fn ctx() -> ConnCtx {
        ConnCtx {
            node_id: "node-a".into(),
            sandbox_id: "sbx-1".into(),
            execution_id: "exec-1".into(),
            template_id: "tmpl".into(),
            port: 40443,
            handler: "http".into(),
            params: serde_json::Value::Null,
            original_dst: Some("93.184.216.34:443".parse().unwrap()),
            egress: Default::default(),
        }
    }

    /// The level is process-wide, so two tests setting it must not overlap.
    #[cfg(feature = "bin")]
    static LEVEL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(feature = "bin")]
    fn captured(level: AuditLevel, write: impl FnOnce()) -> String {
        let _serialized = LEVEL_LOCK.lock().unwrap_or_else(|held| held.into_inner());
        let collector = Collector::default();
        set_level(level);
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(collector.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, write);
        set_level(AuditLevel::Metadata);
        let written = collector.0.lock().unwrap().clone();
        String::from_utf8(written).unwrap()
    }

    #[cfg(feature = "bin")]
    #[test]
    fn a_request_line_names_the_headers_it_injected_and_never_their_values() {
        let ctx = ctx();
        let injected = vec!["authorization".to_string(), "x-api-key".to_string()];
        let line = captured(AuditLevel::Metadata, || {
            request(RequestRecord {
                ctx: &ctx,
                node_id: "node-a",
                host: "api.test",
                method: "POST",
                path: "/v1/models",
                status: 200,
                bytes_in: 12,
                bytes_out: 34,
                latency_ms: 7,
                tls_version: "",
                cipher: "",
                upstream_addr: Some("203.0.113.9:443".parse().unwrap()),
                rule: "*.test",
                injected_headers: &injected,
            });
        });

        assert!(
            line.contains("\"injected_headers\":\"authorization,x-api-key\""),
            "{line}"
        );
        assert!(line.contains("\"sandbox_id\":\"sbx-1\""), "{line}");
        assert!(line.contains("\"dst_ip\":\"93.184.216.34\""), "{line}");
        assert!(line.contains("\"rule\":\"*.test\""), "{line}");
        assert!(line.contains(TARGET), "the line carries its target: {line}");
        // Nothing a credential could be hiding in.
        for forbidden in ["sk-", "Bearer", "?", "key="] {
            assert!(
                !line.contains(forbidden),
                "{forbidden} reached the trail: {line}"
            );
        }
    }

    #[cfg(feature = "bin")]
    #[test]
    fn level_none_writes_no_line_at_all_including_security_events() {
        let ctx = ctx();
        let line = captured(AuditLevel::None, || {
            request(RequestRecord {
                ctx: &ctx,
                node_id: "node-a",
                host: "api.test",
                method: "GET",
                path: "/",
                status: 200,
                bytes_in: 0,
                bytes_out: 0,
                latency_ms: 0,
                tls_version: "",
                cipher: "",
                upstream_addr: None,
                rule: "api.test",
                injected_headers: &[],
            });
            security_event(
                &ctx,
                "node-a",
                "policy_denied",
                "api.test",
                "internet_disabled",
            );
            tls_handshake(&ctx, "node-a", "api.test", "guest", "handshake failure");
        });

        assert!(line.is_empty(), "{line}");
    }

    #[test]
    fn a_multibyte_path_is_cut_on_a_character_boundary() {
        let uri: http::Uri = format!("https://api.test/{}", "é".repeat(2048))
            .parse()
            .unwrap();

        let path = audited_path(&uri);

        assert!(path.len() <= MAX_PATH_BYTES);
        assert!(path.is_char_boundary(path.len()));
    }
}
