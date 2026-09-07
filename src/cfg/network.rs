use std::net::Ipv4Addr;

use anyhow::{bail, Context, Result};
use confique::Config;
use ipnetwork::{IpNetwork, Ipv4Network};
use tracing::{info, warn};

pub const NETWORK_MAX_SLOTS: usize = 32768;

/// Destinations sandbox egress never reaches. Not configurable: a deployment
/// that needs one of these opens a subnet of it through
/// `network.egress.allow_internal_cidrs`.
pub const ALWAYS_DENIED_CIDRS: &[&str] = &[
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "::1/128",
    "fc00::/7",
    "fe80::/10",
];

// Part of the snapshot ABI: fresh boots pass this VM/tap link through the
// kernel `ip=` argument, and snapshot resume does not re-run boot args. Do not
// make this configurable unless vmLinkCidr is persisted in snapshot manifests
// and resume handles missing metadata for old snapshots.
const FIXED_NETWORK_VM_LINK_CIDR: &str = "169.254.0.20/30";

#[derive(Debug, Config, Clone)]
pub struct NetworkConfig {
    #[config(nested)]
    pub egress: NetworkEgressConfig,
    #[config(nested)]
    pub internal: NetworkInternalConfig,
}

#[derive(Debug, Config, Clone)]
pub struct NetworkEgressConfig {
    /// Subnets of [`ALWAYS_DENIED_CIDRS`] this node lets sandbox egress policy
    /// decide about. Each entry must sit inside one table entry; anything else
    /// is a startup error.
    #[config(
        default = [],
        env = "AENV_NETWORK_EGRESS_ALLOW_INTERNAL_CIDRS",
        parse_env = confique::env::parse::list_by_comma
    )]
    pub allow_internal_cidrs: Vec<String>,
    /// Refused at startup. The table it used to shrink is [`ALWAYS_DENIED_CIDRS`];
    /// name a subnet in `allow_internal_cidrs` instead.
    pub always_denied_cidrs: Option<Vec<String>>,
}

impl NetworkEgressConfig {
    /// The always-denied table with every configured hole removed, in both
    /// address families. Callers that install IPv4 rules filter by family.
    pub fn effective_denied_cidrs(&self) -> Result<Vec<IpNetwork>> {
        let holes = self.parsed_allow_internal_cidrs()?;
        Ok(subtract_cidrs(&parsed_always_denied(), &holes))
    }

    fn parsed_allow_internal_cidrs(&self) -> Result<Vec<IpNetwork>> {
        self.allow_internal_cidrs
            .iter()
            .map(|cidr| {
                let network = cidr.parse::<IpNetwork>().with_context(|| {
                    format!("invalid network.egress.allow_internal_cidrs entry {cidr:?}")
                })?;
                if !parsed_always_denied()
                    .iter()
                    .any(|denied| contains_network(*denied, network))
                {
                    bail!(
                        "network.egress.allow_internal_cidrs entry {cidr:?} is not inside the \
                         always-denied table {ALWAYS_DENIED_CIDRS:?}"
                    );
                }
                Ok(network)
            })
            .collect()
    }
}

fn parsed_always_denied() -> Vec<IpNetwork> {
    ALWAYS_DENIED_CIDRS
        .iter()
        .map(|cidr| {
            cidr.parse::<IpNetwork>()
                .expect("the always-denied table parses")
        })
        .collect()
}

/// Whether every address of `inner` is an address of `outer`.
fn contains_network(outer: IpNetwork, inner: IpNetwork) -> bool {
    match (outer, inner) {
        (IpNetwork::V4(outer), IpNetwork::V4(inner)) => {
            outer.prefix() <= inner.prefix() && outer.contains(inner.network())
        }
        (IpNetwork::V6(outer), IpNetwork::V6(inner)) => {
            outer.prefix() <= inner.prefix() && outer.contains(inner.network())
        }
        _ => false,
    }
}

/// `denied` with every address of `holes` removed, expressed as CIDRs. A hole
/// that splits a table entry leaves the sibling prefixes behind, so the result
/// covers exactly the addresses the node still rejects.
fn subtract_cidrs(denied: &[IpNetwork], holes: &[IpNetwork]) -> Vec<IpNetwork> {
    let mut out = Vec::new();
    for network in denied {
        subtract_into(*network, holes, &mut out);
    }
    out
}

fn subtract_into(network: IpNetwork, holes: &[IpNetwork], out: &mut Vec<IpNetwork>) {
    if holes.iter().any(|hole| contains_network(*hole, network)) {
        return;
    }
    if !holes
        .iter()
        .any(|hole| contains_network(network, *hole) && hole.prefix() > network.prefix())
    {
        out.push(network);
        return;
    }
    for half in split_once(network) {
        subtract_into(half, holes, out);
    }
}

/// The two prefixes one bit longer that together cover `network`.
fn split_once(network: IpNetwork) -> [IpNetwork; 2] {
    match network {
        IpNetwork::V4(net) => {
            let prefix = net.prefix() + 1;
            let base = u32::from(net.network());
            let step = 1u32 << (32 - prefix);
            [base, base + step].map(|addr| {
                IpNetwork::V4(
                    ipnetwork::Ipv4Network::new(std::net::Ipv4Addr::from(addr), prefix)
                        .expect("a longer prefix of a valid network is valid"),
                )
            })
        }
        IpNetwork::V6(net) => {
            let prefix = net.prefix() + 1;
            let base = u128::from(net.network());
            let step = 1u128 << (128 - prefix);
            [base, base + step].map(|addr| {
                IpNetwork::V6(
                    ipnetwork::Ipv6Network::new(std::net::Ipv6Addr::from(addr), prefix)
                        .expect("a longer prefix of a valid network is valid"),
                )
            })
        }
    }
}

#[derive(Debug, Config, Clone)]
pub struct NetworkInternalConfig {
    /// CIDR used for per-slot host interaction addresses.
    #[config(default = "10.11.0.0/16")]
    pub host_interaction_cidr: String,
    /// CIDR used for per-slot namespace veth pairs.
    #[config(default = "10.12.0.0/16")]
    pub veth_cidr: String,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolvedNetworkInternalConfig {
    pub host_interaction_cidr: Ipv4Network,
    pub veth_cidr: Ipv4Network,
    pub vm_link_cidr: Ipv4Network,
}

/// The IPv4 addresses this machine's interfaces carry. Best-effort: a host
/// whose interfaces cannot be listed is warned about nothing, because the
/// alternative is a startup that fails on a machine it cannot inspect.
fn host_ipv4_addresses() -> Vec<Ipv4Addr> {
    let Ok(interfaces) = nix::ifaddrs::getifaddrs() else {
        return Vec::new();
    };
    interfaces
        .filter_map(|interface| Some(interface.address?.as_sockaddr_in()?.ip()))
        .filter(|address| !address.is_loopback())
        .collect()
}

/// Which of `addresses` each opened range covers. An IPv6 range matches
/// nothing here, and does not need to: IPv6 is off in the sandbox namespace,
/// so the only address a sandbox can reach this host at is an IPv4 one.
fn addresses_inside(holes: &[IpNetwork], addresses: &[Ipv4Addr]) -> Vec<(Ipv4Addr, IpNetwork)> {
    let mut inside = Vec::new();
    for hole in holes {
        for address in addresses {
            if hole.contains(std::net::IpAddr::V4(*address)) {
                inside.push((*address, *hole));
            }
        }
    }
    inside
}

impl NetworkConfig {
    pub fn validate(config: &Self) -> Result<()> {
        if config.egress.always_denied_cidrs.is_some() {
            bail!(
                "network.egress.always_denied_cidrs is refused: the table is now the constant \
                 {ALWAYS_DENIED_CIDRS:?}. Name the subnets this deployment still needs in \
                 network.egress.allow_internal_cidrs"
            );
        }
        let holes = config.egress.parsed_allow_internal_cidrs()?;
        for hole in &holes {
            info!(
                cidr = %hole,
                "sandbox egress policy decides about this otherwise always-denied range"
            );
        }
        for (address, hole) in addresses_inside(&holes, &host_ipv4_addresses()) {
            warn!(
                %address,
                cidr = %hole,
                "an address of this host is inside a range sandbox egress policy may open; a \
                 sandbox whose allowOut names it reaches this machine"
            );
        }

        Self::resolved_internal(config)?;
        Ok(())
    }

    pub fn resolved_internal(config: &Self) -> Result<ResolvedNetworkInternalConfig> {
        let host_interaction_cidr = config
            .internal
            .host_interaction_cidr
            .as_str()
            .parse::<Ipv4Network>()
            .context("invalid network.internal.host_interaction_cidr")?;
        let veth_cidr = config
            .internal
            .veth_cidr
            .as_str()
            .parse::<Ipv4Network>()
            .context("invalid network.internal.veth_cidr")?;
        let vm_link_cidr = FIXED_NETWORK_VM_LINK_CIDR
            .parse::<Ipv4Network>()
            .context("invalid fixed VM link CIDR")?;
        if vm_link_cidr.prefix() != 30 {
            bail!("fixed VM link CIDR must be a /30 network");
        }

        let max_slots = NETWORK_MAX_SLOTS as u32;
        let max_slot_index = max_slots - 1;
        if host_interaction_cidr.size() < max_slots {
            bail!(
                "network.internal.host_interaction_cidr ({host_interaction_cidr}) must contain at least {max_slots} addresses to cover slot indexes 1..={max_slot_index}; slot 0 is reserved"
            );
        }
        if veth_cidr.size() < max_slots * 2 {
            bail!(
                "network.internal.veth_cidr ({veth_cidr}) must contain at least {} addresses to cover two veth addresses per slot through slot {max_slot_index}",
                max_slots * 2
            );
        }

        for (left_name, left, right_name, right) in [
            (
                "network.internal.host_interaction_cidr",
                host_interaction_cidr,
                "network.internal.veth_cidr",
                veth_cidr,
            ),
            (
                "network.internal.host_interaction_cidr",
                host_interaction_cidr,
                "fixed VM link CIDR",
                vm_link_cidr,
            ),
            (
                "network.internal.veth_cidr",
                veth_cidr,
                "fixed VM link CIDR",
                vm_link_cidr,
            ),
        ] {
            if left.overlaps(right) {
                bail!("{left_name} ({left}) must not overlap {right_name} ({right})");
            }
        }

        Ok(ResolvedNetworkInternalConfig {
            host_interaction_cidr,
            veth_cidr,
            vm_link_cidr,
        })
    }
}

super::impl_config_default!(NetworkConfig, NetworkEgressConfig, NetworkInternalConfig);

pub fn normalize_dns_name(domain: &str) -> Option<String> {
    let domain = domain.to_ascii_lowercase();
    is_valid_dns_name(&domain).then_some(domain)
}

fn is_valid_dns_name(domain: &str) -> bool {
    const MAX_DNS_NAME_LEN: usize = 253;
    !domain.is_empty()
        && domain.len() <= MAX_DNS_NAME_LEN
        && domain.split('.').all(is_valid_dns_label)
}

fn is_valid_dns_label(label: &str) -> bool {
    const MAX_DNS_LABEL_LEN: usize = 63;
    let bytes = label.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_DNS_LABEL_LEN {
        return false;
    }
    if !matches!(bytes[0], b'a'..=b'z' | b'0'..=b'9')
        || !matches!(bytes[bytes.len() - 1], b'a'..=b'z' | b'0'..=b'9')
    {
        return false;
    }
    bytes
        .iter()
        .skip(1)
        .take(bytes.len().saturating_sub(2))
        .all(|byte| matches!(*byte, b'a'..=b'z' | b'0'..=b'9' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_opened_range_that_covers_a_host_address_is_reported() {
        let holes = vec![
            "10.42.0.0/16".parse::<IpNetwork>().unwrap(),
            "192.168.9.0/24".parse::<IpNetwork>().unwrap(),
        ];
        let addresses = [
            Ipv4Addr::new(10, 42, 0, 7),
            Ipv4Addr::new(203, 0, 113, 4),
            Ipv4Addr::new(192, 168, 9, 1),
        ];

        let inside = addresses_inside(&holes, &addresses);

        assert_eq!(
            inside,
            vec![
                (Ipv4Addr::new(10, 42, 0, 7), holes[0]),
                (Ipv4Addr::new(192, 168, 9, 1), holes[1]),
            ],
            "a public address is not inside anything, and both host addresses are"
        );
        assert!(addresses_inside(&holes, &[]).is_empty());
        assert!(addresses_inside(&[], &addresses).is_empty());
    }

    #[test]
    fn validate_accepts_custom_network_config() -> Result<()> {
        let config = NetworkConfig {
            egress: NetworkEgressConfig {
                allow_internal_cidrs: vec!["10.42.0.0/16".to_string()],
                always_denied_cidrs: None,
            },
            internal: NetworkInternalConfig {
                host_interaction_cidr: "100.64.0.0/16".to_string(),
                veth_cidr: "100.65.0.0/16".to_string(),
            },
        };

        NetworkConfig::validate(&config)
    }

    fn egress(allow_internal: &[&str]) -> NetworkEgressConfig {
        NetworkEgressConfig {
            allow_internal_cidrs: allow_internal.iter().map(|s| s.to_string()).collect(),
            always_denied_cidrs: None,
        }
    }

    fn config_with(egress: NetworkEgressConfig) -> NetworkConfig {
        NetworkConfig {
            egress,
            internal: NetworkInternalConfig {
                host_interaction_cidr: "10.11.0.0/16".to_string(),
                veth_cidr: "10.12.0.0/16".to_string(),
            },
        }
    }

    #[test]
    fn an_allow_internal_entry_outside_the_table_is_refused() {
        let err = NetworkConfig::validate(&config_with(egress(&["8.8.8.0/24"])))
            .expect_err("a public range is not inside the always-denied table");

        assert!(
            format!("{err:#}").contains("allow_internal_cidrs"),
            "{err:#}"
        );
    }

    #[test]
    fn the_removed_table_key_is_refused_rather_than_ignored() {
        let mut config = config_with(egress(&[]));
        config.egress.always_denied_cidrs = Some(vec!["10.0.0.0/8".to_string()]);

        let err = NetworkConfig::validate(&config).expect_err("the key is gone");

        assert!(
            format!("{err:#}").contains("allow_internal_cidrs"),
            "the refusal points at the replacement: {err:#}"
        );
    }

    #[test]
    fn a_hole_leaves_the_rest_of_its_table_entry_denied() {
        let denied = egress(&["10.42.0.0/16"])
            .effective_denied_cidrs()
            .expect("the hole is inside 10.0.0.0/8");
        let rendered: Vec<String> = denied.iter().map(ToString::to_string).collect();

        assert!(!rendered.contains(&"10.0.0.0/8".to_string()));
        assert!(rendered.contains(&"10.43.0.0/16".to_string()));
        assert!(rendered.contains(&"192.168.0.0/16".to_string()));
        assert!(rendered.contains(&"fc00::/7".to_string()));
        assert!(!denied
            .iter()
            .any(|net| net.contains("10.42.7.1".parse::<std::net::IpAddr>().unwrap())));
        assert!(denied
            .iter()
            .any(|net| net.contains("10.255.255.254".parse::<std::net::IpAddr>().unwrap())));
    }

    #[test]
    fn without_a_hole_the_effective_table_is_the_constant_table() {
        let denied = egress(&[]).effective_denied_cidrs().expect("no holes");

        assert_eq!(
            denied.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ALWAYS_DENIED_CIDRS
                .iter()
                .map(|cidr| cidr.parse::<IpNetwork>().unwrap().to_string())
                .collect::<Vec<_>>()
        );
    }
}
