use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::{IpAddr, Ipv4Addr},
};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use crate::cfg::network::normalize_dns_name;

pub const ALL_INTERNET_TRAFFIC_CIDR: &str = "0.0.0.0/0";

const EGRESS_CHAIN: &str = "AGENTENV-EGRESS";
const USER_EGRESS_CHAIN: &str = "AGENTENV-USER-EGRESS";
/// One chain of this name in the nat table (the DNAT to the brokered
/// listener) and one in the filter table (the UDP reject that forces
/// HTTP/3 back onto TCP). Both are flushed whenever the intercept changes.
pub const INTERCEPT_CHAIN: &str = "AGENTENV-INTERCEPT";

/// The handler every `rules` declaration normalizes to.
pub const HTTP_BROKER_HANDLER: &str = "http";
/// The destination port the `rules` intercept captures.
pub const HTTPS_PORT: u16 = 443;

/// Marker prefixes accepted inside a transform header value.
pub const SECRET_MARKER_PREFIXES: [&str; 2] = ["${aenv.secrets.", "${e2b.secrets."];
pub const MAX_SECRET_NAMES_PER_DOMAIN: usize = 32;
pub const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;
pub const MAX_SECRET_NAME_LEN: usize = 128;

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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkEgressPolicy {
    pub allowed_cidrs: Vec<String>,
    pub allowed_domains: Vec<String>,
    pub denied_cidrs: Vec<String>,
    /// The public shape: per-domain transform rules as the API received them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rules: BTreeMap<String, Vec<DomainRule>>,
    /// The internal shape derived from `rules`; never accepted from the API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokeredEndpoint>,
}

impl SandboxNetworkEgressPolicy {
    pub fn new(allow_out: Option<Vec<String>>, deny_out: Option<Vec<String>>) -> Result<Self> {
        Self::with_rules(allow_out, deny_out, None)
    }

    /// Validates every domain key, header name and marker, then derives
    /// `brokers` from the rules. Any non-empty `rules` becomes one `http`
    /// endpoint intercepting port 443.
    pub fn with_rules(
        allow_out: Option<Vec<String>>,
        deny_out: Option<Vec<String>>,
        rules: Option<BTreeMap<String, Vec<DomainRule>>>,
    ) -> Result<Self> {
        let mut policy = Self::base(allow_out, deny_out)?;
        let mut normalized_rules = BTreeMap::new();
        for (domain, domain_rules) in rules.unwrap_or_default() {
            let key = normalize_domain_pattern(&domain)
                .with_context(|| format!("invalid rules domain {domain:?}"))?;
            validate_domain_rules(&key, &domain_rules)?;
            if normalized_rules.insert(key.clone(), domain_rules).is_some() {
                bail!("rules domain {key:?} is declared more than once");
            }
        }
        if !normalized_rules.is_empty() {
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
        Ok(policy)
    }

    /// Every secret name any rule refers to, for grant issuance.
    pub fn referenced_secret_names(&self) -> BTreeSet<String> {
        self.rules
            .values()
            .flatten()
            .flat_map(|rule| rule.transform.headers.values())
            .flat_map(|value| secret_markers(value))
            .map(|marker| marker.name.to_string())
            .collect()
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
    }

    pub fn has_brokers(&self) -> bool {
        !self.brokers.is_empty()
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

    pub fn has_domain_allow_rules(&self) -> bool {
        self.egress.has_domain_allow_rules()
    }
}

pub fn set_namespace_egress_policy(policy: Option<&SandboxNetworkPolicy>) -> Result<()> {
    let default_policy = SandboxNetworkPolicy::default();
    let policy = policy.unwrap_or(&default_policy);

    if !policy.egress.allowed_domains.is_empty() {
        bail!(
            "domain entries in allowOut require the TCP egress proxy, which is not enabled yet: {:?}",
            policy.egress.allowed_domains
        );
    }

    let commands = build_user_egress_commands(policy, true);
    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
}

fn configured_always_denied_cidrs() -> &'static [String] {
    &crate::cfg::ConfigManager::global_config()
        .network
        .egress
        .always_denied_cidrs
}

/// Installs the namespace FORWARD egress chains and the anti-spoofing DROP.
/// The DROP is inserted last so it ends up ahead of the egress chain jump.
pub fn initialize_namespace_egress_chain(
    veth_host_ip: Ipv4Addr,
    guest_dns_ip: Ipv4Addr,
    vm_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
) -> Result<()> {
    let commands = build_namespace_egress_chain_commands(
        veth_host_ip,
        guest_dns_ip,
        vm_ip,
        internal_egress_denied_cidrs,
        configured_always_denied_cidrs(),
    );

    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
        .context("initialize AgentENV namespace egress iptables chains")
}

fn build_namespace_egress_chain_commands(
    veth_host_ip: Ipv4Addr,
    guest_dns_ip: Ipv4Addr,
    vm_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
    node_always_denied_cidrs: &[String],
) -> Vec<IptablesRestoreCommand> {
    let mut commands = vec![
        IptablesRestoreCommand::NewChain {
            table: "filter",
            chain: EGRESS_CHAIN,
        },
        IptablesRestoreCommand::NewChain {
            table: "filter",
            chain: USER_EGRESS_CHAIN,
        },
        IptablesRestoreCommand::NewChain {
            table: "filter",
            chain: INTERCEPT_CHAIN,
        },
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "FORWARD",
            position: 1,
            rule: format!("-i tap0 -o vpeer -j {EGRESS_CHAIN}"),
        },
        IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: EGRESS_CHAIN,
        },
        IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: INTERCEPT_CHAIN,
        },
    ];
    commands.extend(build_static_egress_commands(
        veth_host_ip,
        guest_dns_ip,
        internal_egress_denied_cidrs,
        node_always_denied_cidrs,
    ));
    // Every FORWARD ACCEPT above matches on destination only; a guest packet
    // carrying another source address must never reach them. Inserting at
    // position 1 after the egress chain jump keeps the DROP ahead of it.
    commands.push(anti_spoof_forward_command(vm_ip));
    commands.extend([
        IptablesRestoreCommand::NewChain {
            table: "nat",
            chain: INTERCEPT_CHAIN,
        },
        IptablesRestoreCommand::FlushChain {
            table: "nat",
            chain: INTERCEPT_CHAIN,
        },
        IptablesRestoreCommand::Append {
            table: "nat",
            chain: "PREROUTING",
            rule: format!("-i tap0 -j {INTERCEPT_CHAIN}"),
        },
    ]);

    commands
}

/// Routes the guest's traffic to `dports` onto the brokered listener at
/// `listener` and rejects UDP to the same ports so HTTP/3 falls back to TCP.
/// Replaces whatever intercept was installed before.
pub fn install_namespace_intercept(listener: std::net::SocketAddrV4, dports: &[u16]) -> Result<()> {
    let commands = build_intercept_commands(listener, dports);
    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
        .context("install AgentENV namespace intercept")
}

/// Leaves the intercept chains empty; the guest's traffic is forwarded as
/// its egress policy says.
pub fn remove_namespace_intercept() -> Result<()> {
    apply_iptables_commands(
        &build_remove_intercept_commands(),
        OpenFailurePolicy::ReturnErr,
    )
    .context("remove AgentENV namespace intercept")
}

fn build_intercept_commands(
    listener: std::net::SocketAddrV4,
    dports: &[u16],
) -> Vec<IptablesRestoreCommand> {
    let mut commands = build_remove_intercept_commands();
    for dport in dports {
        commands.push(IptablesRestoreCommand::Append {
            table: "filter",
            chain: INTERCEPT_CHAIN,
            rule: format!("-i tap0 -o vpeer -p udp --dport {dport} -j REJECT"),
        });
    }
    for dport in dports {
        commands.push(IptablesRestoreCommand::Append {
            table: "nat",
            chain: INTERCEPT_CHAIN,
            rule: format!(
                "-i tap0 -p tcp --dport {dport} -j DNAT --to-destination {}:{}",
                listener.ip(),
                listener.port()
            ),
        });
    }
    commands.sort_by_key(|command| command.table());
    commands
}

fn build_remove_intercept_commands() -> Vec<IptablesRestoreCommand> {
    vec![
        IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: INTERCEPT_CHAIN,
        },
        IptablesRestoreCommand::FlushChain {
            table: "nat",
            chain: INTERCEPT_CHAIN,
        },
    ]
}

fn anti_spoof_forward_command(vm_ip: Ipv4Addr) -> IptablesRestoreCommand {
    IptablesRestoreCommand::Insert {
        table: "filter",
        chain: "FORWARD",
        position: 1,
        rule: format!("-i tap0 ! -s {vm_ip} -j DROP"),
    }
}

fn build_static_egress_commands(
    veth_host_ip: Ipv4Addr,
    guest_dns_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
    node_always_denied_cidrs: &[String],
) -> Vec<IptablesRestoreCommand> {
    let mut commands = vec![append_egress_command(format!(
        "-i tap0 -o vpeer -j {INTERCEPT_CHAIN}"
    ))];

    for cidr in [format!("{veth_host_ip}/32"), format!("{guest_dns_ip}/32")] {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j ACCEPT"
        )));
    }

    // Internal AgentENV networks are denied before user rules so a sandbox
    // cannot reach another sandbox's namespace or VM link addresses.
    for cidr in internal_egress_denied_cidrs
        .iter()
        .chain(node_always_denied_cidrs.iter())
    {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j REJECT"
        )));
    }

    commands.push(append_egress_command(format!(
        "-i tap0 -o vpeer -j {USER_EGRESS_CHAIN}"
    )));

    commands
}

fn build_user_egress_commands(
    policy: &SandboxNetworkPolicy,
    replace: bool,
) -> Vec<IptablesRestoreCommand> {
    let mut commands = if replace {
        vec![IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: USER_EGRESS_CHAIN,
        }]
    } else {
        Vec::new()
    };

    for cidr in policy.egress.allowed_cidrs.iter().map(String::as_str) {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j ACCEPT"
        )));
    }

    for cidr in policy.egress.denied_cidrs.iter().map(String::as_str) {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j REJECT"
        )));
    }

    if policy.base_policy == BaseSandboxNetworkPolicy::Deny {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {ALL_INTERNET_TRAFFIC_CIDR} -j REJECT"
        )));
    }

    commands
}

fn append_egress_command(rule: String) -> IptablesRestoreCommand {
    IptablesRestoreCommand::Append {
        table: "filter",
        chain: EGRESS_CHAIN,
        rule,
    }
}

fn append_user_egress_command(rule: String) -> IptablesRestoreCommand {
    IptablesRestoreCommand::Append {
        table: "filter",
        chain: USER_EGRESS_CHAIN,
        rule,
    }
}

fn try_normalize_ip_or_cidr(s: &str) -> Result<Option<String>> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Some(match ip {
            IpAddr::V4(ip) => format!("{ip}/32"),
            IpAddr::V6(ip) => format!("{ip}/128"),
        }));
    }

    match s.parse::<ipnetwork::IpNetwork>() {
        Ok(network) => Ok(Some(network.to_string())),
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

fn validate_domain_rules(domain: &str, rules: &[DomainRule]) -> Result<()> {
    let mut names = BTreeSet::new();
    for rule in rules {
        for (header, value) in &rule.transform.headers {
            if !is_valid_header_name(header) {
                bail!("rules for {domain:?} name an invalid header {header:?}");
            }
            if value.len() > MAX_HEADER_VALUE_BYTES {
                bail!(
                    "rules for {domain:?} set header {header:?} to a value over {MAX_HEADER_VALUE_BYTES} bytes"
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
        }
    }
    if names.len() > MAX_SECRET_NAMES_PER_DOMAIN {
        bail!(
            "rules for {domain:?} reference {} secret names; at most {MAX_SECRET_NAMES_PER_DOMAIN} are allowed",
            names.len()
        );
    }
    Ok(())
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
    use crate::cfg::NetworkConfig;

    fn append_rule(command: &IptablesRestoreCommand) -> Option<&str> {
        match command {
            IptablesRestoreCommand::Append { rule, .. } => Some(rule.as_str()),
            _ => None,
        }
    }

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
    fn build_rules_keeps_allow_before_deny() {
        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress: SandboxNetworkEgressPolicy {
                allowed_cidrs: vec!["8.8.8.8/32".to_string()],
                denied_cidrs: Vec::new(),
                allowed_domains: Vec::new(),
                ..Default::default()
            },
        };

        let commands = build_user_egress_commands(&policy, false);

        let allow_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 8.8.8.8/32 -j ACCEPT")
            })
            .unwrap();
        let deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 0.0.0.0/0 -j REJECT")
            })
            .unwrap();
        assert!(allow_pos < deny_pos);
    }

    #[test]
    fn build_policy_replacement_flushes_before_installing_rules() {
        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress: SandboxNetworkEgressPolicy {
                allowed_cidrs: vec!["8.8.8.8/32".to_string()],
                denied_cidrs: vec!["203.0.113.0/24".to_string()],
                allowed_domains: Vec::new(),
                ..Default::default()
            },
        };

        let commands = build_user_egress_commands(&policy, true);

        assert!(matches!(
            commands.first(),
            Some(IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: USER_EGRESS_CHAIN,
            })
        ));
        assert_eq!(
            commands.iter().filter_map(append_rule).collect::<Vec<_>>(),
            [
                "-i tap0 -o vpeer -d 8.8.8.8/32 -j ACCEPT",
                "-i tap0 -o vpeer -d 203.0.113.0/24 -j REJECT",
                "-i tap0 -o vpeer -d 0.0.0.0/0 -j REJECT",
            ]
        );
    }

    #[test]
    fn build_default_policy_replacement_only_flushes_user_chain() {
        let commands = build_user_egress_commands(&SandboxNetworkPolicy::default(), true);

        assert!(matches!(
            commands.as_slice(),
            [IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: USER_EGRESS_CHAIN,
            }]
        ));
    }

    #[test]
    fn build_static_rules_include_baseline_in_order() {
        let denied_cidrs = NetworkConfig::default().egress.always_denied_cidrs;
        let internal_egress_denied_cidrs = Vec::new();
        let commands = build_static_egress_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            &internal_egress_denied_cidrs,
            &denied_cidrs,
        );

        let host_allow_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 10.12.0.2/32 -j ACCEPT")
            })
            .unwrap();
        assert!(commands.iter().any(
            |command| append_rule(command) == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -j ACCEPT")
        ));
        let hard_deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
            })
            .unwrap();
        let shared_deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 100.64.0.0/10 -j REJECT")
            })
            .unwrap();
        let user_chain_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -j AGENTENV-USER-EGRESS")
            })
            .unwrap();

        assert!(host_allow_pos < hard_deny_pos);
        assert!(hard_deny_pos < shared_deny_pos);
        assert!(hard_deny_pos < user_chain_pos);
        assert!(shared_deny_pos < user_chain_pos);
    }

    #[test]
    fn anti_spoof_drop_is_inserted_at_position_one_after_the_egress_chain_jump() {
        let commands = build_namespace_egress_chain_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            Ipv4Addr::new(10, 12, 0, 3),
            &[],
            &[],
        );

        let forward_inserts: Vec<(usize, i32, &str)> = commands
            .iter()
            .enumerate()
            .filter_map(|(index, command)| match command {
                IptablesRestoreCommand::Insert {
                    table: "filter",
                    chain: "FORWARD",
                    position,
                    rule,
                } => Some((index, *position, rule.as_str())),
                _ => None,
            })
            .collect();

        let last_filter_index = commands
            .iter()
            .rposition(|command| command.table() == "filter")
            .unwrap();
        assert_eq!(
            forward_inserts,
            [
                (3, 1, "-i tap0 -o vpeer -j AGENTENV-EGRESS"),
                (last_filter_index, 1, "-i tap0 ! -s 10.12.0.3 -j DROP"),
            ]
        );
    }

    #[test]
    fn build_static_rules_use_configured_denied_cidrs() {
        let denied_cidrs = vec!["203.0.113.0/24".to_string()];
        let internal_egress_denied_cidrs = Vec::new();
        let commands = build_static_egress_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            &internal_egress_denied_cidrs,
            &denied_cidrs,
        );

        assert!(commands.iter().any(|command| {
            append_rule(command) == Some("-i tap0 -o vpeer -d 203.0.113.0/24 -j REJECT")
        }));
        assert!(!commands.iter().any(
            |command| append_rule(command) == Some("-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
        ));
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
            broker.params["rules"]["api.openai.com"][0]["transform"]["headers"]["Authorization"],
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
    fn the_init_sequence_creates_both_intercept_chains_and_jumps_to_them_first() {
        let commands = build_namespace_egress_chain_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            Ipv4Addr::new(10, 12, 0, 3),
            &[],
            &[],
        );
        assert!(commands.windows(2).all(|w| w[0].table() <= w[1].table()));
        assert!(commands.iter().any(|c| matches!(
            c,
            IptablesRestoreCommand::NewChain {
                table: "filter",
                chain: INTERCEPT_CHAIN
            }
        )));
        assert!(commands.iter().any(|c| matches!(
            c,
            IptablesRestoreCommand::NewChain {
                table: "nat",
                chain: INTERCEPT_CHAIN
            }
        )));
        let first_egress_rule = commands
            .iter()
            .find_map(|c| match c {
                IptablesRestoreCommand::Append {
                    table: "filter",
                    chain: EGRESS_CHAIN,
                    rule,
                } => Some(rule.as_str()),
                _ => None,
            })
            .unwrap();
        assert_eq!(first_egress_rule, "-i tap0 -o vpeer -j AGENTENV-INTERCEPT");
        assert!(commands.iter().any(|c| matches!(
            c,
            IptablesRestoreCommand::Append { table: "nat", chain: "PREROUTING", rule }
                if rule == "-i tap0 -j AGENTENV-INTERCEPT"
        )));
    }

    #[test]
    fn intercept_commands_flush_then_reject_udp_and_dnat_tcp_grouped_by_table() {
        let listener = "169.254.0.22:40443".parse().unwrap();
        let commands = build_intercept_commands(listener, &[443]);
        assert!(commands.windows(2).all(|w| w[0].table() <= w[1].table()));

        let rendered: Vec<String> = commands
            .iter()
            .map(|c| match c {
                IptablesRestoreCommand::FlushChain { table, chain } => {
                    format!("{table} -F {chain}")
                }
                IptablesRestoreCommand::Append { table, chain, rule } => {
                    format!("{table} -A {chain} {rule}")
                }
                _ => unreachable!("intercept uses only flush and append"),
            })
            .collect();
        assert_eq!(
            rendered,
            [
                "filter -F AGENTENV-INTERCEPT",
                "filter -A AGENTENV-INTERCEPT -i tap0 -o vpeer -p udp --dport 443 -j REJECT",
                "nat -F AGENTENV-INTERCEPT",
                "nat -A AGENTENV-INTERCEPT -i tap0 -p tcp --dport 443 -j DNAT --to-destination 169.254.0.22:40443",
            ]
        );

        let removal = build_remove_intercept_commands();
        assert_eq!(removal.len(), 2);
        assert!(removal.iter().all(|c| matches!(
            c,
            IptablesRestoreCommand::FlushChain {
                chain: INTERCEPT_CHAIN,
                ..
            }
        )));
    }
}
