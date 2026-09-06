//! The excluded set every node intermediate carries.
//!
//! An excluded set and not a permitted one: a permitted set would have to
//! enumerate the public internet, and a rule may name any host on it. What
//! must never be signable is a name or address inside the cluster, which is
//! finite and is what this lists.

use anyhow::{Context, Result};
use openssl::x509::{X509Extension, X509v3Context};

/// DNS suffixes no leaf under a node intermediate may carry. A leading dot is
/// the RFC 5280 spelling for "this name and everything under it".
pub const EXCLUDED_DOMAINS: [&str; 4] = [".svc", ".cluster.local", ".local", ".internal"];

/// Address ranges no leaf may carry, as (CIDR, netmask) — OpenSSL's name
/// constraints take a full netmask, not a prefix length.
pub const EXCLUDED_NETWORKS: [(&str, &str); 5] = [
    ("10.0.0.0", "255.0.0.0"),
    ("172.16.0.0", "255.240.0.0"),
    ("192.168.0.0", "255.255.0.0"),
    ("100.64.0.0", "255.192.0.0"),
    ("169.254.0.0", "255.255.0.0"),
];

/// The extension value OpenSSL's v3 parser reads.
pub fn name_constraints_value() -> String {
    let mut parts = vec!["critical".to_string()];
    for domain in EXCLUDED_DOMAINS {
        parts.push(format!("excluded;DNS:{domain}"));
    }
    for (network, mask) in EXCLUDED_NETWORKS {
        parts.push(format!("excluded;IP:{network}/{mask}"));
    }
    parts.join(",")
}

pub(super) fn extension(context: &X509v3Context<'_>) -> Result<X509Extension> {
    #[allow(deprecated)]
    X509Extension::new_nid(
        None,
        Some(context),
        openssl::nid::Nid::NAME_CONSTRAINTS,
        &name_constraints_value(),
    )
    .context("build the nameConstraints extension")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_value_is_critical_and_excludes_every_listed_name() {
        let value = name_constraints_value();

        assert!(value.starts_with("critical,"), "{value}");
        for domain in EXCLUDED_DOMAINS {
            assert!(
                value.contains(&format!("excluded;DNS:{domain}")),
                "{domain}: {value}"
            );
        }
        for (network, mask) in EXCLUDED_NETWORKS {
            assert!(
                value.contains(&format!("excluded;IP:{network}/{mask}")),
                "{network}: {value}"
            );
        }
        assert!(!value.contains("permitted"), "{value}");
    }

    #[test]
    fn every_excluded_network_carries_a_full_netmask() {
        for (network, mask) in EXCLUDED_NETWORKS {
            assert!(
                network.parse::<std::net::Ipv4Addr>().is_ok(),
                "{network} is not an address"
            );
            let mask: std::net::Ipv4Addr = mask.parse().expect("a netmask, not a prefix length");
            let bits = u32::from(mask);
            assert_eq!(
                bits.leading_ones() + bits.trailing_zeros(),
                32,
                "{mask} is not contiguous"
            );
        }
    }
}
