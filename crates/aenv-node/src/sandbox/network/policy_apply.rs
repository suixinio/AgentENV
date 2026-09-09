//! The iptables a node writes into a sandbox namespace from an egress policy.
//!
//! The policy itself -- what a request may declare and what it normalizes to --
//! is the model in `aenv-core` that both halves read. Everything here shells out
//! to `iptables` inside a namespace, which only a machine that runs sandboxes
//! has.

use std::net::Ipv4Addr;

use anyhow::{bail, Context, Result};
use tracing::{debug, warn};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use super::policy::{BaseSandboxNetworkPolicy, SandboxNetworkPolicy};

pub const ALL_INTERNET_TRAFFIC_CIDR: &str = "0.0.0.0/0";

const EGRESS_CHAIN: &str = "AGENTENV-EGRESS";
const USER_EGRESS_CHAIN: &str = "AGENTENV-USER-EGRESS";
/// One chain of this name in the nat table (the DNAT to the brokered
/// listener) and one in the filter table (the UDP reject that forces
/// HTTP/3 back onto TCP). Both are flushed whenever the intercept changes.
pub const INTERCEPT_CHAIN: &str = "AGENTENV-INTERCEPT";

/// The only port the guest reaches its resolver on.
const DNS_PORT: u16 = 53;

/// Tags the counting rule for guest packets addressed to the node itself.
const GUEST_TO_NODE_COMMENT: &str = "aenv-guest-to-node";

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

#[cfg(test)]
mod tests {
    use super::super::policy::SandboxNetworkEgressPolicy;
    use super::*;
    use crate::cfg::NetworkConfig;

    fn append_rule(command: &IptablesRestoreCommand) -> Option<&str> {
        match command {
            IptablesRestoreCommand::Append { rule, .. } => Some(rule.as_str()),
            _ => None,
        }
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
