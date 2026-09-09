use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::{IpAddr, Ipv4Addr},
};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use tracing::{debug, warn};

use crate::cfg::network::normalize_dns_name;
use crate::secret_kind::SecretKind;

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

/// The only port the guest reaches its resolver on.
const DNS_PORT: u16 = 53;

/// Tags the counting rule for guest packets addressed to the node itself.
const GUEST_TO_NODE_COMMENT: &str = "aenv-guest-to-node";

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

/// The always-denied table this node installs, in the one family the namespace
/// carries; the table's IPv6 entries are covered by disabling IPv6 outright.
fn configured_always_denied_cidrs() -> Vec<String> {
    match crate::cfg::ConfigManager::global_config()
        .network
        .egress
        .effective_denied_cidrs()
    {
        Ok(denied) => denied
            .into_iter()
            .filter(|network| network.is_ipv4())
            .map(|network| network.to_string())
            .collect(),
        Err(err) => {
            // Validation refuses this configuration at startup; reaching it here
            // means falling back to the whole table rather than to none of it.
            warn!(error = %format_args!("{err:#}"), "using the unmodified always-denied table");
            crate::cfg::network::ALWAYS_DENIED_CIDRS
                .iter()
                .filter(|cidr| !cidr.contains(':'))
                .map(|cidr| (*cidr).to_string())
                .collect()
        }
    }
}

/// Logs how many guest packets this namespace addressed to the node itself.
/// Must run inside the sandbox namespace; does nothing unless debug is on.
pub fn log_guest_to_node_probe_counters() {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }

    let counters = crate::privileges::run_with_scoped_capabilities(
        &[crate::privileges::CAP_NET_ADMIN],
        || {
            let output = std::process::Command::new("iptables")
                .args(["-t", "filter", "-L", EGRESS_CHAIN, "-v", "-x", "-n"])
                .output()
                .context("list the namespace egress chain")?;
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        },
    );

    match counters {
        Ok(listing) => {
            for line in listing
                .lines()
                .filter(|line| line.contains(GUEST_TO_NODE_COMMENT))
            {
                debug!(rule = line.trim(), "guest packets addressed to the node");
            }
        }
        Err(err) => {
            debug!(error = %format_args!("{err:#}"), "cannot read the guest-to-node counter")
        }
    }
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
        &configured_always_denied_cidrs(),
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

/// One brokered listener and the guest destination ports DNATed onto it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterceptTarget {
    pub listener: std::net::SocketAddrV4,
    pub dports: Vec<u16>,
}

/// Replaces the namespace's intercept with exactly `targets`. Every target
/// is installed in one pass because the chains are flushed first: installing
/// them one at a time would leave only the last one in place.
pub fn install_namespace_intercept(targets: &[InterceptTarget]) -> Result<()> {
    let commands = build_intercept_commands(targets);
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

fn build_intercept_commands(targets: &[InterceptTarget]) -> Vec<IptablesRestoreCommand> {
    let mut commands = build_remove_intercept_commands();
    for target in targets {
        for dport in &target.dports {
            commands.push(IptablesRestoreCommand::Append {
                table: "filter",
                chain: INTERCEPT_CHAIN,
                rule: format!("-i tap0 -o vpeer -p udp --dport {dport} -j REJECT"),
            });
            // A DNATed packet is delivered locally and never forwarded, so TCP
            // reaching this chain is TCP the DNAT below did not catch: the guest
            // must not get to the destination unbrokered.
            commands.push(IptablesRestoreCommand::Append {
                table: "filter",
                chain: INTERCEPT_CHAIN,
                rule: format!("-i tap0 -o vpeer -p tcp --dport {dport} -j REJECT"),
            });
        }
    }
    for target in targets {
        for dport in &target.dports {
            commands.push(IptablesRestoreCommand::Append {
                table: "nat",
                chain: INTERCEPT_CHAIN,
                rule: format!(
                    "-i tap0 -p tcp --dport {dport} -j DNAT --to-destination {}:{}",
                    target.listener.ip(),
                    target.listener.port()
                ),
            });
        }
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
    // First, and it is the reply path of every connection the node makes
    // into the sandbox: envd's readiness poll, command execution and file
    // injection all reach the guest through the slot's host-interaction
    // address, and the guest answers to `veth_host_ip`. Nothing below accepts
    // that, so without this line a sandbox never reaches `running`.
    //
    // It admits no new connection: the guest's own outbound traffic is `NEW`
    // and still falls through to the rules below.
    let mut commands = vec![append_egress_command(
        "-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT".to_string(),
    )];

    commands.push(append_egress_command(format!(
        "-i tap0 -o vpeer -j {INTERCEPT_CHAIN}"
    )));

    // The node's own address is not an egress destination the guest may open:
    // brokered traffic is DNATed in nat PREROUTING before this chain runs, and
    // the reply path above has already been taken. The rule names no target so
    // it only counts, and a guest that opens something new towards the node
    // shows up as a non-zero counter rather than as a working connection.
    commands.push(append_egress_command(format!(
        "-i tap0 -o vpeer -d {veth_host_ip}/32 -m conntrack --ctstate NEW -m comment --comment \"{GUEST_TO_NODE_COMMENT}\""
    )));

    for protocol in ["udp", "tcp"] {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {guest_dns_ip}/32 -p {protocol} --dport {DNS_PORT} -j ACCEPT"
        )));
    }

    // Internal AgentENV networks are denied before user rules so a sandbox
    // cannot reach another sandbox's namespace or VM link addresses. The
    // ranges are the address plan's own pools, whole rather than this slot's
    // share of them (`AddressPlan::internal_egress_denied_cidrs`), so moving
    // `[network.internal]` moves what this rejects with it.
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

    fn static_commands() -> Vec<IptablesRestoreCommand> {
        let denied_cidrs: Vec<String> = NetworkConfig::default()
            .egress
            .effective_denied_cidrs()
            .unwrap()
            .into_iter()
            .filter(|network| network.is_ipv4())
            .map(|network| network.to_string())
            .collect();
        build_static_egress_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            &[],
            &denied_cidrs,
        )
    }

    #[test]
    fn the_node_address_is_counted_and_never_accepted() {
        let commands = static_commands();
        let rules: Vec<&str> = commands.iter().filter_map(append_rule).collect();

        let probe = rules
            .iter()
            .find(|rule| rule.contains(GUEST_TO_NODE_COMMENT))
            .expect("the node address is still counted");
        assert!(probe.contains("-d 10.12.0.2/32"), "{probe}");
        assert!(
            probe.contains("--ctstate NEW"),
            "the counter must not count the reply path it no longer owns: {probe}"
        );
        assert!(
            !probe.contains("-j "),
            "the counter names no target: {probe}"
        );
        assert!(
            !rules
                .iter()
                .any(|rule| rule.contains("10.12.0.2/32") && rule.contains("ACCEPT")),
            "no rule accepts a new connection to the node: {rules:?}"
        );
    }

    #[test]
    fn the_reply_path_is_accepted_before_anything_else_in_the_chain() {
        let commands = static_commands();
        let rules: Vec<&str> = commands.iter().filter_map(append_rule).collect();

        let established = rules
            .iter()
            .position(|rule| rule.contains("--ctstate ESTABLISHED,RELATED"))
            .expect("the node's own connections into the sandbox answer through this chain");
        assert_eq!(
            rules[established],
            "-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
        );
        assert_eq!(
            established, 0,
            "the reply path is the first rule: everything below it rejects by destination, and \
             the node's address is one of the destinations they reject"
        );

        let intercept = rules
            .iter()
            .position(|rule| rule.contains(INTERCEPT_CHAIN))
            .expect("the intercept jump is still installed");
        assert!(
            established < intercept,
            "an established reply must not be re-examined by the intercept: {rules:?}"
        );
    }

    #[test]
    fn a_new_connection_to_the_node_still_falls_through_to_a_reject() {
        // The counter names no target, so what stops a guest-opened connection
        // to the node is the range reject below it. Both have to be there, in
        // that order, or the counter is the whole of the policy.
        let rules: Vec<String> = static_commands()
            .iter()
            .filter_map(append_rule)
            .map(ToString::to_string)
            .collect();

        let counted = rules
            .iter()
            .position(|rule| rule.contains(GUEST_TO_NODE_COMMENT))
            .expect("the counter is installed");
        let rejected = rules
            .iter()
            .position(|rule| rule == "-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
            .expect("10.12.0.2 is inside the always-denied 10.0.0.0/8");

        assert!(counted < rejected, "{rules:?}");
    }

    #[test]
    fn only_port_53_reaches_the_resolver() {
        let commands = static_commands();
        // Every ACCEPT that decides a destination. The reply-path rule decides
        // none: it matches on connection state and names no address.
        let accepts: Vec<&str> = commands
            .iter()
            .filter_map(append_rule)
            .filter(|rule| rule.contains("-j ACCEPT"))
            .filter(|rule| !rule.contains("ESTABLISHED,RELATED"))
            .collect();

        assert_eq!(
            accepts,
            [
                "-i tap0 -o vpeer -d 10.1.2.1/32 -p udp --dport 53 -j ACCEPT",
                "-i tap0 -o vpeer -d 10.1.2.1/32 -p tcp --dport 53 -j ACCEPT",
            ]
        );
        assert!(
            accepts.iter().all(|rule| rule.contains("--dport 53")),
            "an ACCEPT without --dport 53 reopens the resolver address: {accepts:?}"
        );
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

    #[test]
    fn build_static_rules_include_baseline_in_order() {
        let denied_cidrs: Vec<String> = NetworkConfig::default()
            .egress
            .effective_denied_cidrs()
            .unwrap()
            .into_iter()
            .filter(|network| network.is_ipv4())
            .map(|network| network.to_string())
            .collect();
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
                append_rule(command).is_some_and(|rule| rule.contains(GUEST_TO_NODE_COMMENT))
            })
            .unwrap();
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

    #[test]
    fn the_init_sequence_creates_both_intercept_chains_and_jumps_to_them_before_any_policy() {
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
        let egress_rules: Vec<&str> = commands
            .iter()
            .filter_map(|c| match c {
                IptablesRestoreCommand::Append {
                    table: "filter",
                    chain: EGRESS_CHAIN,
                    rule,
                } => Some(rule.as_str()),
                _ => None,
            })
            .collect();
        // The reply path is ahead of the intercept — an established answer is
        // not a connection to intercept — and the intercept is ahead of every
        // rule that decides a policy.
        assert_eq!(
            egress_rules[0],
            "-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
        );
        assert_eq!(egress_rules[1], "-i tap0 -o vpeer -j AGENTENV-INTERCEPT");
        assert!(commands.iter().any(|c| matches!(
            c,
            IptablesRestoreCommand::Append { table: "nat", chain: "PREROUTING", rule }
                if rule == "-i tap0 -j AGENTENV-INTERCEPT"
        )));
    }

    #[test]
    fn intercept_commands_flush_then_reject_unbrokered_traffic_and_dnat_tcp_grouped_by_table() {
        let commands = build_intercept_commands(&[InterceptTarget {
            listener: "169.254.0.22:40443".parse().unwrap(),
            dports: vec![443],
        }]);
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
                "filter -A AGENTENV-INTERCEPT -i tap0 -o vpeer -p tcp --dport 443 -j REJECT",
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

    #[test]
    fn every_target_keeps_its_own_dnat_because_the_chains_are_flushed_once() {
        let commands = build_intercept_commands(&[
            InterceptTarget {
                listener: "169.254.0.22:40443".parse().unwrap(),
                dports: vec![443],
            },
            InterceptTarget {
                listener: "169.254.0.22:5432".parse().unwrap(),
                dports: vec![5432],
            },
        ]);
        let dnats: Vec<&str> = commands
            .iter()
            .filter_map(|c| match c {
                IptablesRestoreCommand::Append {
                    table: "nat", rule, ..
                } => Some(rule.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            dnats,
            [
                "-i tap0 -p tcp --dport 443 -j DNAT --to-destination 169.254.0.22:40443",
                "-i tap0 -p tcp --dport 5432 -j DNAT --to-destination 169.254.0.22:5432",
            ]
        );
        assert_eq!(
            commands
                .iter()
                .filter(|c| matches!(c, IptablesRestoreCommand::FlushChain { .. }))
                .count(),
            2,
            "each chain is flushed once for the whole set, never once per target"
        );
    }

    #[test]
    fn an_empty_target_set_leaves_only_the_flush() {
        let commands = build_intercept_commands(&[]);
        assert_eq!(commands.len(), 2);
        assert!(commands.iter().all(|c| matches!(
            c,
            IptablesRestoreCommand::FlushChain {
                chain: INTERCEPT_CHAIN,
                ..
            }
        )));
    }
}
