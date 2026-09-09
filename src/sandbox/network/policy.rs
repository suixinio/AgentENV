use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::IpAddr,
};

use crate::cfg::network::normalize_dns_name;
use crate::secret_kind::SecretKind;

/// The handler every `rules` declaration normalizes to.
pub const HTTP_BROKER_HANDLER: &str = "http";
/// The destination port the `rules` intercept captures.
pub const HTTPS_PORT: u16 = 443;

/// Refusal text for an IPv6 egress entry. The sandbox namespace runs with IPv6
/// disabled, so an accepted entry would be a rule nothing enforces.
const IPV6_UNSUPPORTED: &str = "IPv6 entries are not supported in egress policy";

/// A byte relay to the upstream its params name.
pub const TCP_BROKER_HANDLER: &str = "tcp";
/// A Postgres front end that authenticates upstream with brokered credentials.
pub const POSTGRES_BROKER_HANDLER: &str = "postgres";

/// Handlers an endpoint declaration may name. `http` is derived from `rules`
/// and carries their shape in its params, so it is never declared directly.
pub const DECLARABLE_BROKER_HANDLERS: [&str; 2] = [TCP_BROKER_HANDLER, POSTGRES_BROKER_HANDLER];

/// Ports no endpoint may claim: 443 is the port the `rules` intercept
/// captures, and one listener per port is the whole addressing scheme.
pub const RESERVED_ENDPOINT_PORTS: [u16; 1] = [HTTPS_PORT];

pub const MAX_ENDPOINTS_PER_SANDBOX: usize = 16;

/// Marker prefixes accepted inside a transform header value.
pub const SECRET_MARKER_PREFIXES: [&str; 2] = ["${aenv.secrets.", "${e2b.secrets."];
pub const MAX_SECRET_NAMES_PER_DOMAIN: usize = 32;
pub const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;
pub const MAX_SECRET_NAME_LEN: usize = 128;
/// Cap on the serialized `rules`, half of the 64 KiB identity frame the
/// runtime sends the broker (`aenv_egress::framing::MAX_FRAME_LEN`, which
/// `aenv-core` must not link); the rest carries the sandbox identity and its
/// egress summary.
pub const MAX_RULES_SERIALIZED_BYTES: usize = 32 * 1024;
/// Cap on the serialized `brokers`, which is what actually travels in the
/// identity frame: `rules` reach the broker inside an endpoint's params, and
/// endpoint declarations share the frame with them.
pub const MAX_BROKERS_SERIALIZED_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaseSandboxNetworkPolicy {
    /// Allows outbound traffic except for static namespace egress rejects.
    #[default]
    Default,
    Allow,
    Deny,
}

/// Headers set on every matching request; a header the guest sent under the
/// same name is replaced. Values may carry `${aenv.secrets.NAME}` markers.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HeaderTransform {
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DomainRule {
    #[serde(default)]
    pub transform: HeaderTransform,
}

/// Which guest destination ports are DNATed onto the brokered listener.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Intercept {
    pub dports: Vec<u16>,
}

/// The internal shape `rules` normalizes to: one listener the runtime opens in
/// the sandbox namespace and the handler the broker runs behind it. `port`
/// zero means the runtime picks the listening port.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BrokeredEndpoint {
    pub port: u16,
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub intercept: Option<Intercept>,
}

/// The public shape of an explicit endpoint: a port the runtime listens on
/// inside the sandbox namespace and the handler the broker runs behind it.
/// Nothing is redirected unless `intercept_port` asks for it; a guest reaches
/// the listener by connecting to its address.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointDeclaration {
    pub port: u16,
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub intercept_port: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkEgressPolicy {
    pub allowed_cidrs: Vec<String>,
    pub allowed_domains: Vec<String>,
    pub denied_cidrs: Vec<String>,
    /// The public shape: per-domain transform rules as the API received them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rules: BTreeMap<String, Vec<DomainRule>>,
    /// The public shape: explicit endpoint declarations as the API received
    /// them, normalized.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<EndpointDeclaration>,
    /// The internal shape derived from `rules` and `endpoints`; never accepted
    /// from the API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokeredEndpoint>,
}

impl SandboxNetworkEgressPolicy {
    pub fn new(allow_out: Option<Vec<String>>, deny_out: Option<Vec<String>>) -> Result<Self> {
        Self::with_rules(allow_out, deny_out, None)
    }

    pub fn with_rules(
        allow_out: Option<Vec<String>>,
        deny_out: Option<Vec<String>>,
        rules: Option<BTreeMap<String, Vec<DomainRule>>>,
    ) -> Result<Self> {
        Self::with_rules_and_endpoints(allow_out, deny_out, rules, None)
    }

    /// Validates every domain key, header name and marker, canonicalizes
    /// header names to lowercase, validates each endpoint declaration against
    /// its handler, then derives `brokers` from both. Any non-empty `rules`
    /// becomes one `http` endpoint intercepting port 443; each declaration
    /// becomes one endpoint on the port it names.
    pub fn with_rules_and_endpoints(
        allow_out: Option<Vec<String>>,
        deny_out: Option<Vec<String>>,
        rules: Option<BTreeMap<String, Vec<DomainRule>>>,
        endpoints: Option<Vec<EndpointDeclaration>>,
    ) -> Result<Self> {
        let mut policy = Self::base(allow_out, deny_out)?;
        let mut normalized_rules = BTreeMap::new();
        for (domain, domain_rules) in rules.unwrap_or_default() {
            let key = normalize_domain_pattern(&domain)
                .with_context(|| format!("invalid rules domain {domain:?}"))?;
            let domain_rules = normalize_domain_rules(&key, &domain_rules)?;
            if normalized_rules.insert(key.clone(), domain_rules).is_some() {
                bail!("rules domain {key:?} is declared more than once");
            }
        }
        if !normalized_rules.is_empty() {
            let serialized = serde_json::to_vec(&normalized_rules)
                .context("serialize the rules to size them")?
                .len();
            if serialized > MAX_RULES_SERIALIZED_BYTES {
                bail!(
                    "rules serialize to {serialized} bytes; at most {MAX_RULES_SERIALIZED_BYTES} are allowed, so declare fewer domains, rules or headers"
                );
            }
            policy.brokers = vec![BrokeredEndpoint {
                port: 0,
                handler: HTTP_BROKER_HANDLER.to_string(),
                params: serde_json::json!({ "rules": normalized_rules }),
                intercept: Some(Intercept {
                    dports: vec![HTTPS_PORT],
                }),
            }];
            policy.rules = normalized_rules;
        }
        for declaration in normalize_endpoints(endpoints.unwrap_or_default())? {
            // A header marker reads an opaque value and an endpoint
            // credential reads fields; no stored secret satisfies both.
            if let Some(credential) = declaration
                .params
                .get("credential")
                .and_then(serde_json::Value::as_str)
            {
                let in_a_rule = policy
                    .rules
                    .values()
                    .flatten()
                    .flat_map(|rule| rule.transform.headers.values())
                    .flat_map(|value| secret_markers(value))
                    .any(|marker| marker.name == credential);
                if in_a_rule {
                    bail!(
                        "secret {credential:?} is both an endpoint credential and a header marker; \
                         a secret has one shape, so use two secrets"
                    );
                }
            }
            policy.brokers.push(BrokeredEndpoint {
                port: declaration.port,
                handler: declaration.handler.clone(),
                params: declaration.params.clone(),
                intercept: declaration.intercept_port.then(|| Intercept {
                    dports: vec![declaration.port],
                }),
            });
            policy.endpoints.push(declaration);
        }
        if !policy.brokers.is_empty() {
            let serialized = serde_json::to_vec(&policy.brokers)
                .context("serialize the brokers to size them")?
                .len();
            if serialized > MAX_BROKERS_SERIALIZED_BYTES {
                bail!(
                    "rules and endpoints serialize to {serialized} bytes; at most {MAX_BROKERS_SERIALIZED_BYTES} are allowed, so declare fewer domains, rules, headers or endpoints"
                );
            }
        }
        Ok(policy)
    }

    /// Every secret name the rules and the endpoints refer to, for grant
    /// issuance.
    pub fn referenced_secret_names(&self) -> BTreeSet<String> {
        self.referenced_secrets().into_keys().collect()
    }

    /// Every secret name the policy refers to, with the shape that use
    /// reads: a header marker takes an opaque value, an endpoint credential
    /// the fields a handler takes apart. A name used both ways is refused by
    /// `with_rules_and_endpoints`, so each name maps to one shape.
    pub fn referenced_secrets(&self) -> BTreeMap<String, SecretKind> {
        let from_rules = self
            .rules
            .values()
            .flatten()
            .flat_map(|rule| rule.transform.headers.values())
            .flat_map(|value| secret_markers(value))
            .map(|marker| (marker.name.to_string(), SecretKind::Opaque));
        let from_endpoints = self
            .endpoints
            .iter()
            .filter_map(|endpoint| endpoint.params.get("credential"))
            .filter_map(|credential| credential.as_str())
            .map(|name| (name.to_string(), SecretKind::Fields));
        from_rules.chain(from_endpoints).collect()
    }

    fn base(allow_out: Option<Vec<String>>, deny_out: Option<Vec<String>>) -> Result<Self> {
        let mut policy = Self::default();
        let mut allowed_cidrs = HashSet::new();
        let mut allowed_domains = HashSet::new();
        let mut denied_cidrs = HashSet::new();

        for entry in allow_out.unwrap_or_default() {
            if let Some(cidr) = try_normalize_ip_or_cidr(&entry)? {
                if allowed_cidrs.insert(cidr.clone()) {
                    policy.allowed_cidrs.push(cidr);
                }
            } else {
                let domain = normalize_domain_pattern(&entry)
                    .with_context(|| format!("invalid allowOut domain entry {entry:?}"))?;
                if allowed_domains.insert(domain.clone()) {
                    policy.allowed_domains.push(domain);
                }
            }
        }

        for entry in deny_out.unwrap_or_default() {
            let Some(cidr) = try_normalize_ip_or_cidr(&entry)? else {
                bail!("denyOut entry {entry:?} must be an IP address or CIDR block");
            };
            if denied_cidrs.insert(cidr.clone()) {
                policy.denied_cidrs.push(cidr);
            }
        }

        Ok(policy)
    }

    pub fn has_explicit_rules(&self) -> bool {
        !self.allowed_cidrs.is_empty()
            || !self.allowed_domains.is_empty()
            || !self.denied_cidrs.is_empty()
            || !self.rules.is_empty()
            || !self.endpoints.is_empty()
    }

    pub fn has_brokers(&self) -> bool {
        !self.brokers.is_empty()
    }

    /// Whether any broker terminates TLS the guest opened, so the guest has
    /// to trust the broker's CA. Only `http` does; an explicit endpoint is
    /// reached in the clear inside the namespace.
    pub fn needs_guest_ca(&self) -> bool {
        self.brokers
            .iter()
            .any(|broker| broker.handler == HTTP_BROKER_HANDLER)
    }

    pub fn has_domain_allow_rules(&self) -> bool {
        !self.allowed_domains.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkPolicy {
    pub base_policy: BaseSandboxNetworkPolicy,
    pub egress: SandboxNetworkEgressPolicy,
}

impl SandboxNetworkPolicy {
    pub fn new(base_policy: BaseSandboxNetworkPolicy, egress: SandboxNetworkEgressPolicy) -> Self {
        Self {
            base_policy,
            egress,
        }
    }

    pub fn runtime_policy(&self) -> Option<Self> {
        self.has_runtime_egress_rules().then(|| self.clone())
    }

    pub fn has_explicit_egress_rules(&self) -> bool {
        self.egress.has_explicit_rules()
    }

    pub fn has_runtime_egress_rules(&self) -> bool {
        self.base_policy == BaseSandboxNetworkPolicy::Deny
            || self.has_explicit_egress_rules()
            || self.egress.has_brokers()
    }

    pub fn has_brokers(&self) -> bool {
        self.egress.has_brokers()
    }

    pub fn needs_guest_ca(&self) -> bool {
        self.egress.needs_guest_ca()
    }

    pub fn has_domain_allow_rules(&self) -> bool {
        self.egress.has_domain_allow_rules()
    }
}
fn try_normalize_ip_or_cidr(s: &str) -> Result<Option<String>> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(ip) => Ok(Some(format!("{ip}/32"))),
            IpAddr::V6(_) => bail!("{IPV6_UNSUPPORTED}: {s:?}"),
        };
    }

    match s.parse::<ipnetwork::IpNetwork>() {
        Ok(ipnetwork::IpNetwork::V4(network)) => Ok(Some(network.to_string())),
        Ok(ipnetwork::IpNetwork::V6(_)) => bail!("{IPV6_UNSUPPORTED}: {s:?}"),
        Err(err) if s.contains('/') => {
            Err(err).with_context(|| format!("invalid IP or CIDR entry {s:?}"))
        }
        Err(_) => Ok(None),
    }
}

/// One `${aenv.secrets.NAME}` (or `${e2b.secrets.NAME}`) occurrence inside a
/// header value, with the byte range it occupies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMarker<'a> {
    pub name: &'a str,
    pub range: std::ops::Range<usize>,
}

pub fn is_valid_secret_name(name: &str) -> bool {
    (1..=MAX_SECRET_NAME_LEN).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Finds every well-formed marker in `value`. A `${` that is not one of the
/// known prefixes, or a prefix without a closing `}`, is literal text.
pub fn secret_markers(value: &str) -> Vec<SecretMarker<'_>> {
    let mut markers = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = value[cursor..].find("${") {
        let start = cursor + offset;
        let rest = &value[start..];
        let Some(prefix) = SECRET_MARKER_PREFIXES
            .iter()
            .find(|prefix| rest.starts_with(*prefix))
        else {
            cursor = start + 2;
            continue;
        };
        let name_start = start + prefix.len();
        let Some(close) = value[name_start..].find('}') else {
            break;
        };
        let name = &value[name_start..name_start + close];
        let end = name_start + close + 1;
        if is_valid_secret_name(name) {
            markers.push(SecretMarker {
                name,
                range: start..end,
            });
            cursor = end;
        } else {
            cursor = name_start;
        }
    }
    markers
}

fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

// A byte the broker could not put on the wire: CR, LF, NUL and the rest of
// the C0 range apart from HTAB, plus DEL. Refusing them here keeps a template
// that can never be sent out of the accepted policy.
fn forbidden_header_value_byte(value: &str) -> Option<u8> {
    value
        .bytes()
        .find(|byte| (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f)
}

fn normalize_domain_rules(domain: &str, rules: &[DomainRule]) -> Result<Vec<DomainRule>> {
    let mut names = BTreeSet::new();
    let mut normalized = Vec::with_capacity(rules.len());
    for rule in rules {
        let mut headers = BTreeMap::new();
        for (header, value) in &rule.transform.headers {
            if !is_valid_header_name(header) {
                bail!("rules for {domain:?} name an invalid header {header:?}");
            }
            if value.len() > MAX_HEADER_VALUE_BYTES {
                bail!(
                    "rules for {domain:?} set header {header:?} to a value over {MAX_HEADER_VALUE_BYTES} bytes"
                );
            }
            if let Some(byte) = forbidden_header_value_byte(value) {
                bail!(
                    "rules for {domain:?} set header {header:?} to a value holding the control byte {byte:#04x}; header values carry printable text and tabs only"
                );
            }
            for marker in secret_markers(value) {
                names.insert(marker.name.to_string());
            }
            if let Some(malformed) = malformed_marker_name(value) {
                bail!(
                    "rules for {domain:?} set header {header:?} with a secret marker whose name {malformed:?} is not [a-zA-Z0-9_-]{{1,{MAX_SECRET_NAME_LEN}}}"
                );
            }
            // Header names match case-insensitively at the broker, so the
            // canonical form is what makes later-rule-wins hold across
            // casings.
            if headers
                .insert(header.to_ascii_lowercase(), value.clone())
                .is_some()
            {
                bail!("rules for {domain:?} set header {header:?} more than once, ignoring case");
            }
        }
        normalized.push(DomainRule {
            transform: HeaderTransform { headers },
        });
    }
    if names.len() > MAX_SECRET_NAMES_PER_DOMAIN {
        bail!(
            "rules for {domain:?} reference {} secret names; at most {MAX_SECRET_NAMES_PER_DOMAIN} are allowed",
            names.len()
        );
    }
    Ok(normalized)
}

// A known prefix followed by a closing brace but an invalid name is a typo
// worth refusing rather than a literal worth sending.
fn malformed_marker_name(value: &str) -> Option<&str> {
    let mut cursor = 0;
    while let Some(offset) = value[cursor..].find("${") {
        let start = cursor + offset;
        let rest = &value[start..];
        let Some(prefix) = SECRET_MARKER_PREFIXES
            .iter()
            .find(|prefix| rest.starts_with(*prefix))
        else {
            cursor = start + 2;
            continue;
        };
        let name_start = start + prefix.len();
        let close = value[name_start..].find('}')?;
        let name = &value[name_start..name_start + close];
        if !is_valid_secret_name(name) {
            return Some(name);
        }
        cursor = name_start + close + 1;
    }
    None
}

/// Refuses a declaration whose handler is unknown, whose port collides with
/// the `rules` intercept or with another declaration, or whose params do not
/// fit the handler. Returned params carry only keys the handler knows.
fn normalize_endpoints(declared: Vec<EndpointDeclaration>) -> Result<Vec<EndpointDeclaration>> {
    if declared.len() > MAX_ENDPOINTS_PER_SANDBOX {
        bail!(
            "{} endpoints are declared; at most {MAX_ENDPOINTS_PER_SANDBOX} are allowed",
            declared.len()
        );
    }
    let mut claimed = BTreeSet::new();
    let mut normalized = Vec::with_capacity(declared.len());
    for endpoint in declared {
        if !DECLARABLE_BROKER_HANDLERS.contains(&endpoint.handler.as_str()) {
            bail!(
                "endpoint handler {:?} is not one of {DECLARABLE_BROKER_HANDLERS:?}",
                endpoint.handler
            );
        }
        if endpoint.port == 0 {
            bail!("an endpoint must name a port between 1 and 65535");
        }
        if RESERVED_ENDPOINT_PORTS.contains(&endpoint.port) {
            bail!(
                "endpoint port {} is reserved for the rules intercept",
                endpoint.port
            );
        }
        if !claimed.insert(endpoint.port) {
            bail!("endpoint port {} is declared more than once", endpoint.port);
        }
        let params = normalize_endpoint_params(&endpoint.handler, &endpoint.params)
            .with_context(|| format!("endpoint on port {}", endpoint.port))?;
        normalized.push(EndpointDeclaration { params, ..endpoint });
    }
    Ok(normalized)
}

/// Which params each handler requires and which it merely accepts. A key
/// outside both lists is refused rather than dropped: a misspelled
/// `credential` would otherwise read as an endpoint that needs none.
fn endpoint_param_keys(
    handler: &str,
) -> Result<(&'static [&'static str], &'static [&'static str])> {
    match handler {
        TCP_BROKER_HANDLER => Ok((&["upstream"], &[])),
        POSTGRES_BROKER_HANDLER => Ok((&["credential"], &["upstream_tls"])),
        other => bail!("no endpoint params are defined for handler {other:?}"),
    }
}

fn normalize_endpoint_params(
    handler: &str,
    params: &serde_json::Value,
) -> Result<serde_json::Value> {
    let mut object = match params {
        serde_json::Value::Null => serde_json::Map::new(),
        serde_json::Value::Object(object) => object.clone(),
        _ => bail!("params must be a JSON object"),
    };
    let (required, optional) = endpoint_param_keys(handler)?;
    for key in object.keys() {
        if !required.contains(&key.as_str()) && !optional.contains(&key.as_str()) {
            bail!(
                "params carry unknown key {key:?}; handler {handler:?} takes {required:?} and optionally {optional:?}"
            );
        }
    }
    for key in required {
        if !object.contains_key(*key) {
            bail!("params must carry {key:?} for handler {handler:?}");
        }
    }
    if let Some(upstream) = object.get("upstream") {
        let upstream = upstream
            .as_str()
            .context("params upstream must be a \"host:port\" string")?;
        let canonical = normalize_upstream_authority(upstream)?;
        object.insert("upstream".into(), serde_json::Value::String(canonical));
    }
    if let Some(credential) = object.get("credential") {
        let name = credential
            .as_str()
            .context("params credential must be a secret name")?;
        if !is_valid_secret_name(name) {
            bail!("params credential {name:?} is not a valid secret name");
        }
    }
    if let Some(upstream_tls) = object.get("upstream_tls") {
        if !upstream_tls.is_boolean() {
            bail!("params upstream_tls must be a boolean");
        }
    }
    Ok(serde_json::Value::Object(object))
}

/// `host:port`, where the host is a DNS name or an IP literal and an IPv6
/// literal is bracketed. The canonical form is what the broker dials.
fn normalize_upstream_authority(authority: &str) -> Result<String> {
    let Some((host, port)) = authority.rsplit_once(':') else {
        bail!("upstream {authority:?} must be host:port");
    };
    let port: u16 = port
        .parse()
        .with_context(|| format!("upstream {authority:?} carries an invalid port"))?;
    if port == 0 {
        bail!("upstream {authority:?} must name a port between 1 and 65535");
    }
    let bracketed = host.starts_with('[') && host.ends_with(']');
    let host = if bracketed {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if host.is_empty() {
        bail!("upstream {authority:?} names no host");
    }
    let canonical = match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) if !bracketed => v4.to_string(),
        Ok(IpAddr::V6(v6)) => format!("[{v6}]"),
        Ok(IpAddr::V4(_)) => bail!("upstream {authority:?} brackets an IPv4 address"),
        Err(_) if bracketed => bail!("upstream {authority:?} brackets a host that is not IPv6"),
        Err(_) => normalize_dns_name(host)
            .with_context(|| format!("upstream {authority:?} names an invalid host"))?,
    };
    Ok(format!("{canonical}:{port}"))
}

/// The `rules` key grammar, as an option rather than a `Result`: an exact DNS
/// name or one leading `*.` wildcard, lowercased.
pub fn normalize_host_pattern(pattern: &str) -> Option<String> {
    normalize_domain_pattern(pattern).ok()
}

fn normalize_domain_pattern(pattern: &str) -> Result<String> {
    let (wildcard, domain) = pattern
        .strip_prefix("*.")
        .map(|domain| (true, domain))
        .unwrap_or((false, pattern));
    let Some(domain) = normalize_dns_name(domain) else {
        bail!("invalid DNS domain pattern");
    };
    Ok(if wildcard {
        format!("*.{domain}")
    } else {
        domain
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_splits_allow_cidrs_and_domains() {
        let policy = SandboxNetworkEgressPolicy::new(
            Some(vec![
                "8.8.8.8".to_string(),
                "1.1.1.0/24".to_string(),
                "*.example.com".to_string(),
            ]),
            Some(vec!["203.0.113.0/24".to_string()]),
        )
        .unwrap();

        assert_eq!(policy.allowed_cidrs, ["8.8.8.8/32", "1.1.1.0/24"]);
        assert_eq!(policy.allowed_domains, ["*.example.com"]);
        assert_eq!(policy.denied_cidrs, ["203.0.113.0/24"]);
    }

    #[test]
    fn new_deduplicates_normalized_entries_without_reordering() {
        let policy = SandboxNetworkEgressPolicy::new(
            Some(vec![
                "8.8.8.8".to_string(),
                "1.1.1.1".to_string(),
                "8.8.8.8/32".to_string(),
                "Example.com".to_string(),
                "example.com".to_string(),
            ]),
            Some(vec![
                "203.0.113.1".to_string(),
                "203.0.113.1/32".to_string(),
                "203.0.113.0/24".to_string(),
            ]),
        )
        .unwrap();

        assert_eq!(policy.allowed_cidrs, ["8.8.8.8/32", "1.1.1.1/32"]);
        assert_eq!(policy.allowed_domains, ["example.com"]);
        assert_eq!(policy.denied_cidrs, ["203.0.113.1/32", "203.0.113.0/24"]);
    }

    #[test]
    fn new_sets_base_policy() {
        let policy = SandboxNetworkPolicy::new(
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8/32".to_string()]), None).unwrap(),
        );

        assert_eq!(policy.base_policy, BaseSandboxNetworkPolicy::Deny);
        assert!(policy.egress.denied_cidrs.is_empty());
        assert!(policy.has_runtime_egress_rules());
    }

    #[test]
    fn an_ipv6_egress_entry_is_refused() {
        for entry in ["2001:db8::1", "2001:db8::/32", "::1"] {
            let err = SandboxNetworkEgressPolicy::new(Some(vec![entry.to_string()]), None)
                .expect_err("IPv6 is refused");
            assert!(
                format!("{err:#}").contains("IPv6"),
                "{entry}: {}",
                format_args!("{err:#}")
            );
        }
    }

    fn rules(domain: &str, headers: &[(&str, &str)]) -> BTreeMap<String, Vec<DomainRule>> {
        let mut map = BTreeMap::new();
        map.insert(
            domain.to_string(),
            vec![DomainRule {
                transform: HeaderTransform {
                    headers: headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                },
            }],
        );
        map
    }

    #[test]
    fn rules_normalize_to_one_http_endpoint_intercepting_443() {
        let policy = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules(
                "API.OpenAI.com",
                &[("Authorization", "Bearer ${aenv.secrets.openai}")],
            )),
        )
        .unwrap();

        assert!(policy.rules.contains_key("api.openai.com"));
        assert_eq!(policy.brokers.len(), 1);
        let broker = &policy.brokers[0];
        assert_eq!(broker.port, 0);
        assert_eq!(broker.handler, "http");
        assert_eq!(broker.intercept, Some(Intercept { dports: vec![443] }));
        assert_eq!(
            broker.params["rules"]["api.openai.com"][0]["transform"]["headers"]["authorization"],
            "Bearer ${aenv.secrets.openai}"
        );
        assert!(policy.has_explicit_rules());
        assert!(policy.has_brokers());

        let full = SandboxNetworkPolicy::new(BaseSandboxNetworkPolicy::Default, policy);
        assert!(full.has_runtime_egress_rules());
        assert!(full.runtime_policy().is_some());
    }

    #[test]
    fn a_policy_without_rules_has_no_brokers_and_serializes_without_them() {
        let policy = SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8".into()]), None).unwrap();
        assert!(policy.rules.is_empty());
        assert!(policy.brokers.is_empty());
        let json = serde_json::to_value(&policy).unwrap();
        assert!(json.get("rules").is_none());
        assert!(json.get("brokers").is_none());

        let legacy = serde_json::json!({
            "allowed_cidrs": ["8.8.8.8/32"],
            "allowed_domains": [],
            "denied_cidrs": []
        });
        let decoded: SandboxNetworkEgressPolicy = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded, policy);
    }

    #[test]
    fn rules_round_trip_through_json_with_their_brokers() {
        let policy = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules(
                "*.github.com",
                &[("Authorization", "token ${e2b.secrets.gh}")],
            )),
        )
        .unwrap();
        let json = serde_json::to_string(&policy).unwrap();
        let back: SandboxNetworkEgressPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, policy);
        assert_eq!(back.brokers.len(), 1);
    }

    #[test]
    fn referenced_secret_names_collects_both_marker_prefixes_once() {
        let mut all = rules(
            "api.example.com",
            &[
                (
                    "Authorization",
                    "Basic ${aenv.secrets.user}:${aenv.secrets.pass}",
                ),
                ("X-Token", "${e2b.secrets.user}"),
            ],
        );
        all.extend(rules(
            "*.other.example",
            &[("X-Key", "${aenv.secrets.other}")],
        ));
        let policy = SandboxNetworkEgressPolicy::with_rules(None, None, Some(all)).unwrap();
        assert_eq!(
            policy.referenced_secret_names(),
            ["other", "pass", "user"]
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn markers_are_found_and_unknown_dollar_sequences_stay_literal() {
        let markers =
            secret_markers("x ${aenv.secrets.a} ${HOME} ${e2b.secrets.b_2} ${aenv.secrets.");
        assert_eq!(
            markers
                .iter()
                .map(|m| (m.name, m.range.clone()))
                .collect::<Vec<_>>(),
            vec![("a", 2..19), ("b_2", 28..46)]
        );
        assert!(secret_markers("plain").is_empty());
        assert!(secret_markers("${aenv.secrets.}").is_empty());
    }

    #[test]
    fn invalid_rules_are_refused() {
        let bad_domain = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("*", &[("Authorization", "x")])),
        );
        assert!(bad_domain.is_err());

        let bad_header = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("api.example.com", &[("Bad Header", "x")])),
        );
        assert!(bad_header.is_err());

        let bad_name = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules(
                "api.example.com",
                &[("Authorization", "${aenv.secrets.has/slash}")],
            )),
        );
        assert!(bad_name.unwrap_err().to_string().contains("has/slash"));

        let long_value = "v".repeat(MAX_HEADER_VALUE_BYTES + 1);
        let too_long = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("api.example.com", &[("X", long_value.as_str())])),
        );
        assert!(too_long.is_err());

        let many: String = (0..=MAX_SECRET_NAMES_PER_DOMAIN)
            .map(|i| format!("${{aenv.secrets.n{i}}}"))
            .collect();
        let too_many = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("api.example.com", &[("X", many.as_str())])),
        );
        assert!(too_many.is_err());

        let mut duplicate = rules("API.example.com", &[("X", "1")]);
        duplicate.extend(rules("api.example.com", &[("X", "2")]));
        assert!(SandboxNetworkEgressPolicy::with_rules(None, None, Some(duplicate)).is_err());
    }

    #[test]
    fn header_values_carrying_control_bytes_are_refused() {
        for value in ["a\rb", "a\nb", "a\0b", "a\x1fb", "a\x7fb"] {
            let refused = SandboxNetworkEgressPolicy::with_rules(
                None,
                None,
                Some(rules("api.example.com", &[("X-Token", value)])),
            );
            let err = refused
                .expect_err("a control byte must be refused")
                .to_string();
            assert!(err.contains("control byte"), "{value:?}: {err}");
        }

        let tab = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("api.example.com", &[("X-Token", "a\tb")])),
        )
        .expect("a tab is representable in a header value");
        assert_eq!(
            tab.rules["api.example.com"][0].transform.headers["x-token"],
            "a\tb"
        );
    }

    #[test]
    fn header_names_are_canonicalized_to_lowercase_and_casing_duplicates_refused() {
        let policy = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules("api.example.com", &[("X-Api-Key", "k")])),
        )
        .unwrap();
        let headers = &policy.rules["api.example.com"][0].transform.headers;
        assert_eq!(headers.keys().collect::<Vec<_>>(), ["x-api-key"]);
        assert_eq!(
            policy.brokers[0].params["rules"]["api.example.com"][0]["transform"]["headers"]
                ["x-api-key"],
            "k"
        );

        let collision = SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(rules(
                "api.example.com",
                &[("X-Api-Key", "one"), ("x-api-key", "two")],
            )),
        );
        assert!(collision
            .unwrap_err()
            .to_string()
            .contains("more than once, ignoring case"));
    }

    #[test]
    fn rules_over_the_serialized_cap_are_refused() {
        let value = "v".repeat(MAX_HEADER_VALUE_BYTES);
        let mut many = BTreeMap::new();
        for index in 0..8 {
            many.extend(rules(
                &format!("api{index}.example.com"),
                &[("X-Token", value.as_str())],
            ));
        }
        let err = SandboxNetworkEgressPolicy::with_rules(None, None, Some(many))
            .expect_err("8 KiB values over eight domains exceed the cap")
            .to_string();
        assert!(
            err.contains(&MAX_RULES_SERIALIZED_BYTES.to_string()),
            "{err}"
        );

        let mut few = BTreeMap::new();
        for index in 0..3 {
            few.extend(rules(
                &format!("api{index}.example.com"),
                &[("X-Token", value.as_str())],
            ));
        }
        SandboxNetworkEgressPolicy::with_rules(None, None, Some(few))
            .expect("three domains stay under the cap");
    }

    fn endpoint(port: u16, handler: &str, params: serde_json::Value) -> EndpointDeclaration {
        EndpointDeclaration {
            port,
            handler: handler.to_string(),
            params,
            intercept_port: false,
        }
    }

    fn with_endpoints(endpoints: Vec<EndpointDeclaration>) -> Result<SandboxNetworkEgressPolicy> {
        SandboxNetworkEgressPolicy::with_rules_and_endpoints(None, None, None, Some(endpoints))
    }

    #[test]
    fn a_declared_endpoint_becomes_one_broker_on_the_port_it_names() {
        let policy = with_endpoints(vec![endpoint(
            5432,
            "postgres",
            serde_json::json!({"credential": "db"}),
        )])
        .unwrap();

        assert_eq!(
            policy.brokers,
            vec![BrokeredEndpoint {
                port: 5432,
                handler: "postgres".into(),
                params: serde_json::json!({"credential": "db"}),
                intercept: None,
            }]
        );
        assert_eq!(policy.endpoints.len(), 1);
        assert!(policy.has_brokers());
        assert!(!policy.needs_guest_ca());
    }

    #[test]
    fn intercept_port_is_the_only_thing_that_redirects_a_declared_endpoint() {
        let mut declared = endpoint(5432, "postgres", serde_json::json!({"credential": "db"}));
        assert_eq!(
            with_endpoints(vec![declared.clone()]).unwrap().brokers[0].intercept,
            None
        );

        declared.intercept_port = true;
        assert_eq!(
            with_endpoints(vec![declared]).unwrap().brokers[0].intercept,
            Some(Intercept { dports: vec![5432] })
        );
    }

    #[test]
    fn rules_and_endpoints_coexist_and_only_the_rules_endpoint_needs_the_guest_ca() {
        let policy = SandboxNetworkEgressPolicy::with_rules_and_endpoints(
            None,
            None,
            Some(rules("api.example.com", &[("authorization", "Bearer x")])),
            Some(vec![endpoint(
                5432,
                "postgres",
                serde_json::json!({"credential": "db"}),
            )]),
        )
        .unwrap();

        assert_eq!(policy.brokers.len(), 2);
        assert_eq!(policy.brokers[0].handler, HTTP_BROKER_HANDLER);
        assert_eq!(policy.brokers[1].handler, "postgres");
        assert!(policy.needs_guest_ca());
    }

    #[test]
    fn an_endpoint_credential_joins_the_names_a_grant_must_cover() {
        let policy = SandboxNetworkEgressPolicy::with_rules_and_endpoints(
            None,
            None,
            Some(rules(
                "api.example.com",
                &[("authorization", "Bearer ${aenv.secrets.openai}")],
            )),
            Some(vec![endpoint(
                5432,
                "postgres",
                serde_json::json!({"credential": "tenant_db"}),
            )]),
        )
        .unwrap();

        assert_eq!(
            policy.referenced_secret_names(),
            BTreeSet::from(["openai".to_string(), "tenant_db".to_string()])
        );
        assert_eq!(
            policy.referenced_secrets(),
            BTreeMap::from([
                ("openai".to_string(), SecretKind::Opaque),
                ("tenant_db".to_string(), SecretKind::Fields),
            ]),
            "each use names the shape it reads"
        );
    }

    #[test]
    fn a_name_used_as_both_a_marker_and_a_credential_is_refused() {
        let err = SandboxNetworkEgressPolicy::with_rules_and_endpoints(
            None,
            None,
            Some(rules(
                "api.example.com",
                &[("authorization", "Bearer ${aenv.secrets.db}")],
            )),
            Some(vec![endpoint(
                5432,
                "postgres",
                serde_json::json!({"credential": "db"}),
            )]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("one shape"), "{err}");
    }

    #[test]
    fn invalid_endpoints_are_refused() {
        for (endpoints, expected) in [
            (
                vec![endpoint(5432, "http", serde_json::json!({}))],
                "is not one of",
            ),
            (
                vec![endpoint(5432, "psql", serde_json::json!({}))],
                "is not one of",
            ),
            (
                vec![endpoint(
                    443,
                    "tcp",
                    serde_json::json!({"upstream": "a.test:5432"}),
                )],
                "reserved for the rules intercept",
            ),
            (
                vec![endpoint(
                    0,
                    "tcp",
                    serde_json::json!({"upstream": "a.test:5432"}),
                )],
                "between 1 and 65535",
            ),
            (
                vec![
                    endpoint(5432, "tcp", serde_json::json!({"upstream": "a.test:5432"})),
                    endpoint(5432, "postgres", serde_json::json!({"credential": "db"})),
                ],
                "declared more than once",
            ),
            (
                vec![endpoint(5432, "postgres", serde_json::json!({}))],
                "must carry \"credential\"",
            ),
            (
                vec![endpoint(5432, "tcp", serde_json::json!({}))],
                "must carry \"upstream\"",
            ),
            (
                vec![endpoint(
                    5432,
                    "postgres",
                    serde_json::json!({"credentials": "db"}),
                )],
                "unknown key",
            ),
            (
                vec![endpoint(
                    5432,
                    "postgres",
                    serde_json::json!({"credential": "db", "upstream": "elsewhere:5432"}),
                )],
                "unknown key",
            ),
            (
                vec![endpoint(
                    5432,
                    "postgres",
                    serde_json::json!({"credential": "../escape"}),
                )],
                "not a valid secret name",
            ),
            (
                vec![endpoint(
                    5432,
                    "postgres",
                    serde_json::json!({"credential": "db", "upstream_tls": "yes"}),
                )],
                "must be a boolean",
            ),
            (
                vec![endpoint(
                    5432,
                    "tcp",
                    serde_json::json!({"upstream": "a.test"}),
                )],
                "must be host:port",
            ),
            (
                vec![endpoint(
                    5432,
                    "tcp",
                    serde_json::json!({"upstream": "a.test:0"}),
                )],
                "between 1 and 65535",
            ),
            (
                vec![endpoint(
                    5432,
                    "tcp",
                    serde_json::json!({"upstream": ":5432"}),
                )],
                "names no host",
            ),
            (
                vec![endpoint(
                    5432,
                    "tcp",
                    serde_json::json!({"upstream": "not a host:5432"}),
                )],
                "invalid host",
            ),
            (
                vec![endpoint(5432, "tcp", serde_json::json!("upstream"))],
                "must be a JSON object",
            ),
        ] {
            let err = with_endpoints(endpoints).unwrap_err();
            let message = format!("{err:#}");
            assert!(
                message.contains(expected),
                "expected {expected:?} in {message:?}"
            );
        }
    }

    #[test]
    fn an_upstream_authority_is_canonicalized_before_it_reaches_the_broker() {
        for (declared, canonical) in [
            ("DB.Example.COM:5432", "db.example.com:5432"),
            ("10.0.0.1:5432", "10.0.0.1:5432"),
            ("[2001:DB8::1]:5432", "[2001:db8::1]:5432"),
        ] {
            let policy = with_endpoints(vec![endpoint(
                5432,
                "tcp",
                serde_json::json!({"upstream": declared}),
            )])
            .unwrap();
            assert_eq!(policy.brokers[0].params["upstream"], canonical);
        }
        assert!(with_endpoints(vec![endpoint(
            5432,
            "tcp",
            serde_json::json!({"upstream": "[10.0.0.1]:5432"}),
        )])
        .is_err());
    }

    #[test]
    fn more_endpoints_than_the_cap_are_refused() {
        let endpoints: Vec<_> = (1..=MAX_ENDPOINTS_PER_SANDBOX as u16 + 1)
            .map(|i| {
                endpoint(
                    5000 + i,
                    "postgres",
                    serde_json::json!({"credential": "db"}),
                )
            })
            .collect();
        let message = format!("{:#}", with_endpoints(endpoints).unwrap_err());
        assert!(message.contains("at most"), "{message}");
    }

    #[test]
    fn rules_and_endpoints_share_one_serialized_cap() {
        let headers: Vec<(String, String)> = (0..15)
            .map(|i| (format!("x-pad-{i}"), "v".repeat(2000)))
            .collect();
        let borrowed: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let under_the_rules_cap = rules("api.example.com", &borrowed);
        assert!(SandboxNetworkEgressPolicy::with_rules(
            None,
            None,
            Some(under_the_rules_cap.clone())
        )
        .is_ok());

        let long_host = [&"d".repeat(60)[..]; 4].join(".");
        let endpoints: Vec<_> = (0..MAX_ENDPOINTS_PER_SANDBOX as u16)
            .map(|i| {
                endpoint(
                    5000 + i,
                    "tcp",
                    serde_json::json!({"upstream": format!("{long_host}:5432")}),
                )
            })
            .collect();
        let message = format!(
            "{:#}",
            SandboxNetworkEgressPolicy::with_rules_and_endpoints(
                None,
                None,
                Some(under_the_rules_cap),
                Some(endpoints),
            )
            .unwrap_err()
        );
        assert!(
            message.contains("rules and endpoints serialize to"),
            "{message}"
        );
    }

    #[test]
    fn endpoints_round_trip_through_json_with_their_brokers() {
        let policy = with_endpoints(vec![EndpointDeclaration {
            port: 5432,
            handler: "postgres".into(),
            params: serde_json::json!({"credential": "db", "upstream_tls": false}),
            intercept_port: true,
        }])
        .unwrap();
        let encoded = serde_json::to_string(&policy).unwrap();
        assert_eq!(
            serde_json::from_str::<SandboxNetworkEgressPolicy>(&encoded).unwrap(),
            policy
        );
        assert!(encoded.contains("\"endpoints\""));

        let without = SandboxNetworkEgressPolicy::new(None, None).unwrap();
        assert!(!serde_json::to_string(&without)
            .unwrap()
            .contains("endpoints"));
    }
}
