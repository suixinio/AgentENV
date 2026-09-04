use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ipnetwork::IpNetwork;
use tokio::net::TcpStream;

use crate::header::EgressPolicySummary;

/// Destinations no sandbox may reach through the broker regardless of its own
/// policy, in two classes. The absolute ones are the broker's own host, the
/// link-local range that carries cloud metadata, the families that are not
/// destinations, and whatever the operator added — no configuration reaches
/// those. The overridable ones are merely private, and an operator naming one
/// of them in a per-handler allowlist is the whole point of that allowlist.
#[derive(Clone, Debug)]
pub struct BrokerDenyList {
    absolute: Vec<IpNetwork>,
    overridable: Vec<IpNetwork>,
}

impl Default for BrokerDenyList {
    fn default() -> Self {
        Self {
            absolute: parse_built_in(ABSOLUTE_DENIED_CIDRS),
            overridable: parse_built_in(OVERRIDABLE_DENIED_CIDRS),
        }
    }
}

/// Not reachable through this broker under any configuration.
const ABSOLUTE_DENIED_CIDRS: &[&str] = &[
    "0.0.0.0/8",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "fe80::/10",
    "ff00::/8",
    "::ffff:0:0/96",
    "64:ff9b::/96",
];

/// Denied to a destination the guest chose, reachable to one an operator named
/// for a handler: the ranges real upstreams actually live in.
const OVERRIDABLE_DENIED_CIDRS: &[&str] = &[
    "10.0.0.0/8",
    "100.64.0.0/10",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "fc00::/7",
];

fn parse_built_in(cidrs: &[&str]) -> Vec<IpNetwork> {
    cidrs
        .iter()
        .map(|cidr| cidr.parse::<IpNetwork>().expect("built-in cidrs parse"))
        .collect()
}

impl BrokerDenyList {
    /// No built-in ranges at all; for tests that talk to loopback.
    pub fn empty() -> Self {
        Self {
            absolute: Vec::new(),
            overridable: Vec::new(),
        }
    }

    /// Exactly `cidrs`, all of them absolute.
    pub fn from_cidrs<S: AsRef<str>>(cidrs: &[S]) -> Result<Self, ipnetwork::IpNetworkError> {
        let absolute = cidrs
            .iter()
            .map(|c| c.as_ref().parse::<IpNetwork>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            absolute,
            overridable: Vec::new(),
        })
    }

    /// The defaults plus `extra`. What the operator adds is absolute: naming a
    /// range here is the statement that nothing reaches it, and the cluster's
    /// own Service and Pod CIDRs are what belongs here.
    pub fn with_extra<S: AsRef<str>>(extra: &[S]) -> Result<Self, ipnetwork::IpNetworkError> {
        let mut list = Self::default();
        list.absolute.extend(Self::from_cidrs(extra)?.absolute);
        Ok(list)
    }

    fn absolute_matches(&self, ip: IpAddr) -> bool {
        self.absolute.iter().any(|net| net.contains(ip))
    }

    fn matches(&self, ip: IpAddr) -> bool {
        self.absolute_matches(ip) || self.overridable.iter().any(|net| net.contains(ip))
    }
}

/// Handlers whose upstream is named by the endpoint declaration or by the
/// credential resolver rather than by the guest's own connection. Two things
/// follow: without an operator allowlist they reach nothing, and inside one
/// they reach what it names — including a private range, which is where the
/// databases this exists for actually live — without the sandbox's own CIDR
/// policy having a say, because the sandbox never addressed that upstream.
pub const HANDLERS_REQUIRING_ALLOWLIST: [&str; 2] = ["tcp", "postgres"];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DenyReason {
    #[error("destination is inside a range the broker never reaches")]
    BrokerDenied,
    #[error("destination is outside the operator allowlist for this handler")]
    HandlerDenied,
    #[error("this handler reaches nothing until the operator gives it an allowlist")]
    HandlerUnpinned,
    #[error("destination is inside a range the sandbox policy denies")]
    SandboxDenied,
    #[error("the sandbox policy allows no internet access")]
    InternetDisabled,
    #[error("the sandbox policy carries an entry that is not a cidr")]
    MalformedPolicy,
}

impl DenyReason {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::BrokerDenied => "broker_denied_cidr",
            Self::HandlerDenied => "handler_denied_cidr",
            Self::HandlerUnpinned => "handler_no_allowlist",
            Self::SandboxDenied => "sandbox_denied_cidr",
            Self::InternetDisabled => "internet_disabled",
            Self::MalformedPolicy => "malformed_policy",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error(transparent)]
    Denied(DenyReason),
    #[error("resolving {host}: {source}")]
    Resolve {
        host: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{host} resolved to no addresses")]
    NoAddresses { host: String },
    #[error("connecting to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("connecting to {addr} timed out")]
    ConnectTimeout { addr: SocketAddr },
}

impl UpstreamError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Denied(reason) => reason.reason(),
            Self::Resolve { .. } | Self::NoAddresses { .. } => "upstream_unresolvable",
            Self::Connect { .. } | Self::ConnectTimeout { .. } => "upstream_unreachable",
        }
    }
}

#[async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>>;
}

pub struct SystemResolver;

#[async_trait]
impl Resolver for SystemResolver {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        Ok(tokio::net::lookup_host((host, 0u16))
            .await?
            .map(|addr| addr.ip())
            .collect())
    }
}

/// The one way a handler reaches an upstream. Every address is checked
/// against the broker deny list and the sandbox's own policy, in the order
/// the sandbox namespace applies them, and the connection goes to the
/// checked address itself, never back through a name.
pub struct UpstreamGuard {
    deny: BrokerDenyList,
    allowlists: HashMap<String, Vec<IpNetwork>>,
    resolver: Arc<dyn Resolver>,
    connect_timeout: Duration,
}

impl UpstreamGuard {
    pub fn new(deny: BrokerDenyList) -> Self {
        Self {
            deny,
            allowlists: HashMap::new(),
            resolver: Arc::new(SystemResolver),
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Pins `handler` to `cidrs`. A handler with no allowlist reaches
    /// whatever the deny list and the sandbox policy allow, except for the
    /// handlers in [`HANDLERS_REQUIRING_ALLOWLIST`], which then reach
    /// nothing; an allowlist that parses to no range switches its handler off
    /// the same way.
    pub fn with_allowlist<S: AsRef<str>>(
        mut self,
        handler: &str,
        cidrs: &[S],
    ) -> Result<Self, ipnetwork::IpNetworkError> {
        let cidrs = cidrs
            .iter()
            .map(|cidr| cidr.as_ref().parse::<IpNetwork>())
            .collect::<Result<Vec<_>, _>>()?;
        self.allowlists.insert(handler.to_string(), cidrs);
        Ok(self)
    }

    pub fn with_resolver(mut self, resolver: Arc<dyn Resolver>) -> Self {
        self.resolver = resolver;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, UpstreamError> {
        let ips = self
            .resolver
            .resolve(host)
            .await
            .map_err(|source| UpstreamError::Resolve {
                host: host.to_string(),
                source,
            })?;
        if ips.is_empty() {
            return Err(UpstreamError::NoAddresses {
                host: host.to_string(),
            });
        }
        Ok(ips)
    }

    /// The absolute deny list, then the handler's operator allowlist, then —
    /// for a handler whose upstream the guest itself chose — the overridable
    /// deny list and the sandbox's own allow list, deny list and base policy,
    /// in the order the namespace's FORWARD chains apply them.
    ///
    /// A handler in [`HANDLERS_REQUIRING_ALLOWLIST`] is the other case: its
    /// upstream comes from the endpoint declaration or the credential, the
    /// guest never addressed it, and the operator's allowlist is what bounds
    /// it. An address inside that allowlist is reached without consulting the
    /// sandbox's CIDR policy, which would otherwise force the operator to
    /// publish the upstream's address into the sandbox's own configuration —
    /// the address the whole arrangement exists to keep out of it.
    ///
    /// An IPv4-mapped IPv6 address is checked as the IPv4 address it maps.
    pub fn check(
        &self,
        handler: &str,
        ip: IpAddr,
        policy: &EgressPolicySummary,
    ) -> Result<(), DenyReason> {
        let ip = ip.to_canonical();
        if self.deny.absolute_matches(ip) {
            return Err(DenyReason::BrokerDenied);
        }
        let operator_chosen = HANDLERS_REQUIRING_ALLOWLIST.contains(&handler);
        match self.allowlists.get(handler) {
            Some(pinned) if pinned.iter().any(|net| net.contains(ip)) => {
                if operator_chosen {
                    return Ok(());
                }
            }
            Some(_) => return Err(DenyReason::HandlerDenied),
            None if operator_chosen => return Err(DenyReason::HandlerUnpinned),
            None => {}
        }
        if self.deny.matches(ip) {
            return Err(DenyReason::BrokerDenied);
        }
        let allowed = parse_cidrs(&policy.allowed_cidrs)?;
        if allowed.iter().any(|net| net.contains(ip)) {
            return Ok(());
        }
        let denied = parse_cidrs(&policy.denied_cidrs)?;
        if denied.iter().any(|net| net.contains(ip)) {
            return Err(DenyReason::SandboxDenied);
        }
        if !policy.allow_internet {
            return Err(DenyReason::InternetDisabled);
        }
        Ok(())
    }

    /// Resolves `host`, checks every address it resolved to (one denied
    /// address denies the connection), then connects to one of them. The
    /// address checked is the address dialled, in its canonical form.
    pub async fn connect_checked(
        &self,
        handler: &str,
        host: &str,
        port: u16,
        policy: &EgressPolicySummary,
    ) -> Result<(TcpStream, SocketAddr), UpstreamError> {
        let ips: Vec<IpAddr> = self
            .resolve(host)
            .await?
            .into_iter()
            .map(|ip| ip.to_canonical())
            .collect();
        for ip in &ips {
            self.check(handler, *ip, policy)
                .map_err(UpstreamError::Denied)?;
        }
        let mut last_err = None;
        for ip in ips {
            let addr = SocketAddr::new(ip, port);
            match self.connect(addr).await {
                Ok(stream) => return Ok((stream, addr)),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.expect("resolve returned at least one address"))
    }

    /// Checks and connects to an already-known address, such as a
    /// connection's `original_dst`.
    pub async fn connect_checked_addr(
        &self,
        handler: &str,
        addr: SocketAddr,
        policy: &EgressPolicySummary,
    ) -> Result<TcpStream, UpstreamError> {
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
        self.check(handler, addr.ip(), policy)
            .map_err(UpstreamError::Denied)?;
        self.connect(addr).await
    }

    async fn connect(&self, addr: SocketAddr) -> Result<TcpStream, UpstreamError> {
        match tokio::time::timeout(self.connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(source)) => Err(UpstreamError::Connect { addr, source }),
            Err(_) => Err(UpstreamError::ConnectTimeout { addr }),
        }
    }
}

fn parse_cidrs(entries: &[String]) -> Result<Vec<IpNetwork>, DenyReason> {
    entries
        .iter()
        .map(|entry| entry.parse::<IpNetwork>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DenyReason::MalformedPolicy)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_internet() -> EgressPolicySummary {
        EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec![],
            denied_cidrs: vec![],
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn built_in_ranges_are_denied_before_any_sandbox_policy_is_consulted() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let permissive = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec!["0.0.0.0/0".into()],
            denied_cidrs: vec![],
        };
        for denied in [
            "10.1.2.3",
            "172.16.5.5",
            "192.168.1.1",
            "100.64.0.9",
            "169.254.169.254",
            "127.0.0.1",
            "224.0.0.1",
            "::1",
            "fe80::1",
            "fd00::1",
        ] {
            assert_eq!(
                guard.check("http", ip(denied), &permissive),
                Err(DenyReason::BrokerDenied),
                "{denied}"
            );
        }
        assert_eq!(
            guard.check("http", ip("93.184.216.34"), &permissive),
            Ok(())
        );
        assert_eq!(
            guard.check("http", ip("2606:4700::1111"), &permissive),
            Ok(())
        );
    }

    #[test]
    fn the_operator_can_add_the_service_cidr() {
        let guard = UpstreamGuard::new(
            BrokerDenyList::with_extra(&["10.96.0.0/12", "203.0.113.0/24"]).unwrap(),
        );
        assert_eq!(
            guard.check("http", ip("203.0.113.7"), &open_internet()),
            Err(DenyReason::BrokerDenied)
        );
        assert_eq!(
            guard.check("http", ip("203.0.112.7"), &open_internet()),
            Ok(())
        );
    }

    #[test]
    fn a_sandbox_without_internet_access_reaches_nothing_but_its_allow_list() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let closed = EgressPolicySummary {
            allow_internet: false,
            allowed_cidrs: vec![],
            denied_cidrs: vec![],
        };
        assert_eq!(
            guard.check("http", ip("93.184.216.34"), &closed),
            Err(DenyReason::InternetDisabled)
        );
        assert_eq!(
            guard.check("http", ip("8.8.8.8"), &closed),
            Err(DenyReason::InternetDisabled)
        );

        let with_allow = EgressPolicySummary {
            allowed_cidrs: vec!["8.8.8.8/32".into()],
            ..closed
        };
        assert_eq!(guard.check("http", ip("8.8.8.8"), &with_allow), Ok(()));
        assert_eq!(
            guard.check("http", ip("8.8.4.4"), &with_allow),
            Err(DenyReason::InternetDisabled)
        );
    }

    #[test]
    fn sandbox_allow_entries_win_over_sandbox_deny_entries_but_not_over_the_broker_list() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let policy = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec!["203.0.113.10/32".into(), "10.0.0.0/8".into()],
            denied_cidrs: vec!["203.0.113.0/24".into()],
        };
        assert_eq!(guard.check("http", ip("203.0.113.10"), &policy), Ok(()));
        assert_eq!(
            guard.check("http", ip("203.0.113.11"), &policy),
            Err(DenyReason::SandboxDenied)
        );
        assert_eq!(
            guard.check("http", ip("10.2.3.4"), &policy),
            Err(DenyReason::BrokerDenied)
        );
    }

    #[test]
    fn a_policy_entry_that_is_not_a_cidr_fails_closed() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let policy = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec![],
            denied_cidrs: vec!["example.com".into()],
        };
        assert_eq!(
            guard.check("http", ip("93.184.216.34"), &policy),
            Err(DenyReason::MalformedPolicy)
        );
    }

    #[test]
    fn ipv4_mapped_ipv6_addresses_are_checked_as_the_ipv4_address_they_map() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let permissive = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec!["0.0.0.0/0".into(), "::/0".into()],
            denied_cidrs: vec![],
        };
        for denied in [
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
        ] {
            assert_eq!(
                guard.check("http", ip(denied), &permissive),
                Err(DenyReason::BrokerDenied),
                "{denied}"
            );
        }
        assert_eq!(
            guard.check("http", ip("2606:4700::1111"), &permissive),
            Ok(())
        );
        assert_eq!(
            guard.check("http", ip("::ffff:93.184.216.34"), &permissive),
            Ok(())
        );

        let unlisted = UpstreamGuard::new(BrokerDenyList::empty());
        let sandbox = EgressPolicySummary {
            allow_internet: true,
            allowed_cidrs: vec![],
            denied_cidrs: vec!["203.0.113.0/24".into()],
        };
        assert_eq!(
            unlisted.check("http", ip("::ffff:203.0.113.5"), &sandbox),
            Err(DenyReason::SandboxDenied)
        );
    }

    #[test]
    fn the_mapped_and_nat64_prefixes_are_in_the_built_in_deny_list() {
        let deny = BrokerDenyList::default();
        assert!(deny.matches(ip("::ffff:8.8.8.8")));
        assert!(deny.matches(ip("64:ff9b::808:808")));
        assert!(!deny.matches(ip("2606:4700::1111")));
        assert_eq!(
            UpstreamGuard::new(deny).check("http", ip("64:ff9b::a00:1"), &open_internet()),
            Err(DenyReason::BrokerDenied)
        );
    }

    struct FixedResolver(Vec<IpAddr>);

    #[async_trait]
    impl Resolver for FixedResolver {
        async fn resolve(&self, _: &str) -> std::io::Result<Vec<IpAddr>> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn connect_checked_dials_the_canonical_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let guard = UpstreamGuard::new(BrokerDenyList::empty())
            .with_resolver(Arc::new(FixedResolver(vec![ip("::ffff:127.0.0.1")])));

        let (stream, addr) = guard
            .connect_checked("http", "mapped.example", bound.port(), &open_internet())
            .await
            .unwrap();
        assert_eq!(addr, bound);
        let (_accepted, peer) = listener.accept().await.unwrap();
        assert_eq!(peer, stream.local_addr().unwrap());
    }

    #[tokio::test]
    async fn connect_checked_addr_dials_the_canonical_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let guard = UpstreamGuard::new(BrokerDenyList::empty());
        let mapped: SocketAddr = format!("[::ffff:127.0.0.1]:{}", bound.port())
            .parse()
            .unwrap();

        let stream = guard
            .connect_checked_addr("http", mapped, &open_internet())
            .await
            .unwrap();
        assert_eq!(stream.peer_addr().unwrap(), bound);
    }

    #[tokio::test]
    async fn one_denied_address_among_several_denies_the_whole_connection() {
        let guard = UpstreamGuard::new(BrokerDenyList::default()).with_resolver(Arc::new(
            FixedResolver(vec![ip("93.184.216.34"), ip("10.0.0.5")]),
        ));
        let err = guard
            .connect_checked("http", "rebinding.example", 443, &open_internet())
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            UpstreamError::Denied(DenyReason::BrokerDenied)
        ));
        assert_eq!(err.reason(), "broker_denied_cidr");
    }

    #[tokio::test]
    async fn a_name_that_resolves_to_nothing_is_unresolvable() {
        let guard = UpstreamGuard::new(BrokerDenyList::default())
            .with_resolver(Arc::new(FixedResolver(vec![])));
        let err = guard
            .connect_checked("http", "nowhere.example", 443, &open_internet())
            .await
            .err()
            .unwrap();
        assert!(matches!(err, UpstreamError::NoAddresses { .. }));
        assert_eq!(err.reason(), "upstream_unresolvable");
    }

    #[tokio::test]
    async fn connect_checked_connects_to_the_checked_address_not_the_name() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let guard = UpstreamGuard::new(BrokerDenyList::empty())
            .with_resolver(Arc::new(FixedResolver(vec![bound.ip()])));

        let (stream, addr) = guard
            .connect_checked("http", "anything.example", bound.port(), &open_internet())
            .await
            .unwrap();
        assert_eq!(addr, bound);
        let (_accepted, peer) = listener.accept().await.unwrap();
        assert_eq!(peer, stream.local_addr().unwrap());
    }

    #[tokio::test]
    async fn connect_checked_addr_applies_the_same_check_to_an_original_destination() {
        let guard = UpstreamGuard::new(BrokerDenyList::default());
        let err = guard
            .connect_checked_addr(
                "http",
                "169.254.169.254:80".parse().unwrap(),
                &open_internet(),
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            UpstreamError::Denied(DenyReason::BrokerDenied)
        ));

        let closed = EgressPolicySummary {
            allow_internet: false,
            ..open_internet()
        };
        let err = guard
            .connect_checked_addr("http", "93.184.216.34:443".parse().unwrap(), &closed)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            UpstreamError::Denied(DenyReason::InternetDisabled)
        ));
    }
}
