//! Leaf certificates for intercepted names, signed by the cluster CA the
//! guests trust. A leaf is minted only after a rule matched the name; a
//! name no rule covers is never signed.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509Builder, X509NameBuilder, X509};

#[derive(Debug, thiserror::Error)]
pub enum SignError {
    #[error("{0:?} is not a name a certificate can be issued for")]
    InvalidName(String),
    #[error("the sandbox exceeded its leaf certificate budget for this minute")]
    RateLimited,
    #[error("openssl: {0}")]
    Openssl(#[from] openssl::error::ErrorStack),
    #[error("native-tls: {0}")]
    NativeTls(#[from] native_tls::Error),
}

impl SignError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidName(_) => "invalid_sni",
            Self::RateLimited => "leaf_rate_limited",
            Self::Openssl(_) | Self::NativeTls(_) => "leaf_signing_failed",
        }
    }
}

/// A minted leaf, ready to accept connections for its name.
pub struct Leaf {
    pub name: String,
    pub acceptor: tokio_native_tls::TlsAcceptor,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct SignerOptions {
    pub leaf_ttl: Duration,
    pub cache_capacity: usize,
    pub mints_per_sandbox_per_minute: u32,
}

impl Default for SignerOptions {
    fn default() -> Self {
        Self {
            leaf_ttl: Duration::from_secs(24 * 3600),
            cache_capacity: 4096,
            mints_per_sandbox_per_minute: 60,
        }
    }
}

pub struct CaSigner {
    key: PKey<Private>,
    cert: X509,
    cert_pem: Vec<u8>,
    options: SignerOptions,
    cache: Mutex<LeafCache>,
    budgets: Mutex<HashMap<String, (u64, u32)>>,
}

struct LeafCache {
    by_name: HashMap<String, Arc<Leaf>>,
    order: VecDeque<String>,
}

impl CaSigner {
    pub fn from_pem(
        ca_cert_pem: &[u8],
        ca_key_pem: &[u8],
        options: SignerOptions,
    ) -> Result<Self, SignError> {
        let cert = X509::from_pem(ca_cert_pem)?;
        let key = PKey::private_key_from_pem(ca_key_pem)?;
        Ok(Self {
            key,
            cert,
            cert_pem: ca_cert_pem.to_vec(),
            options,
            cache: Mutex::new(LeafCache {
                by_name: HashMap::new(),
                order: VecDeque::new(),
            }),
            budgets: Mutex::new(HashMap::new()),
        })
    }

    /// The CA certificate as guests receive it.
    pub fn ca_cert_pem(&self) -> &[u8] {
        &self.cert_pem
    }

    /// A leaf for `name`, from the cache when one is live, otherwise minted
    /// against `sandbox_id`'s per-minute budget.
    pub fn leaf_for(&self, name: &str, sandbox_id: &str) -> Result<Arc<Leaf>, SignError> {
        if !is_certifiable_name(name) {
            return Err(SignError::InvalidName(name.to_string()));
        }
        let now = SystemTime::now();
        {
            let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(leaf) = cache.by_name.get(name) {
                if leaf.expires_at > now + Duration::from_secs(60) {
                    return Ok(Arc::clone(leaf));
                }
                cache.by_name.remove(name);
            }
        }
        self.charge_budget(sandbox_id, now)?;
        let leaf = Arc::new(self.mint(name, now)?);
        #[cfg(feature = "tls")]
        metrics::counter!("egress_tls_leaf_minted_total").increment(1);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        while cache.by_name.len() >= self.options.cache_capacity {
            match cache.order.pop_front() {
                Some(oldest) => {
                    cache.by_name.remove(&oldest);
                }
                None => break,
            }
        }
        cache.by_name.insert(name.to_string(), Arc::clone(&leaf));
        cache.order.push_back(name.to_string());
        Ok(leaf)
    }

    fn charge_budget(&self, sandbox_id: &str, now: SystemTime) -> Result<(), SignError> {
        let minute = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60)
            .unwrap_or(0);
        let mut budgets = self.budgets.lock().unwrap_or_else(|e| e.into_inner());
        budgets.retain(|_, (at, _)| *at >= minute.saturating_sub(1));
        let entry = budgets.entry(sandbox_id.to_string()).or_insert((minute, 0));
        if entry.0 != minute {
            *entry = (minute, 0);
        }
        if entry.1 >= self.options.mints_per_sandbox_per_minute {
            return Err(SignError::RateLimited);
        }
        entry.1 += 1;
        Ok(())
    }

    fn mint(&self, name: &str, now: SystemTime) -> Result<Leaf, SignError> {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
        let leaf_key = PKey::from_ec_key(EcKey::generate(&group)?)?;

        let mut subject = X509NameBuilder::new()?;
        subject.append_entry_by_nid(Nid::COMMONNAME, name)?;
        let subject = subject.build();

        let mut builder = X509Builder::new()?;
        builder.set_version(2)?;
        let mut serial = BigNum::new()?;
        serial.rand(127, MsbOption::MAYBE_ZERO, false)?;
        builder.set_serial_number(serial.to_asn1_integer()?.as_ref())?;
        builder.set_subject_name(&subject)?;
        builder.set_issuer_name(self.cert.subject_name())?;
        builder.set_pubkey(&leaf_key)?;
        builder
            .set_not_before(Asn1Time::from_unix(unix_secs(now).saturating_sub(300))?.as_ref())?;
        let expires_at = now + self.options.leaf_ttl;
        builder.set_not_after(Asn1Time::from_unix(unix_secs(expires_at))?.as_ref())?;
        builder.append_extension(BasicConstraints::new().critical().build()?)?;
        builder.append_extension(
            KeyUsage::new()
                .critical()
                .digital_signature()
                .key_encipherment()
                .build()?,
        )?;
        builder.append_extension(ExtendedKeyUsage::new().server_auth().build()?)?;
        let san = SubjectAlternativeName::new()
            .dns(name)
            .build(&builder.x509v3_context(Some(&self.cert), None))?;
        builder.append_extension(san)?;
        builder.sign(&self.key, MessageDigest::sha256())?;
        let leaf_cert = builder.build();

        let mut chain_pem = leaf_cert.to_pem()?;
        chain_pem.extend_from_slice(&self.cert_pem);
        let key_pem = leaf_key.private_key_to_pem_pkcs8()?;
        let identity = native_tls::Identity::from_pkcs8(&chain_pem, &key_pem)?;
        let acceptor = native_tls::TlsAcceptor::builder(identity)
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()?;
        Ok(Leaf {
            name: name.to_string(),
            acceptor: tokio_native_tls::TlsAcceptor::from(acceptor),
            expires_at,
        })
    }
}

fn unix_secs(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// DNS names only: labels of letters, digits and hyphens, no wildcard, no
/// empty label, at most 253 bytes.
pub fn is_certifiable_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// A throwaway CA for tests and local runs: returns (cert PEM, PKCS#8 key PEM).
pub fn generate_test_ca(common_name: &str) -> Result<(Vec<u8>, Vec<u8>), SignError> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
    let key = PKey::from_ec_key(EcKey::generate(&group)?)?;
    let mut subject = X509NameBuilder::new()?;
    subject.append_entry_by_nid(Nid::COMMONNAME, common_name)?;
    let subject = subject.build();
    let mut builder = X509Builder::new()?;
    builder.set_version(2)?;
    let mut serial = BigNum::new()?;
    serial.rand(127, MsbOption::MAYBE_ZERO, false)?;
    builder.set_serial_number(serial.to_asn1_integer()?.as_ref())?;
    builder.set_subject_name(&subject)?;
    builder.set_issuer_name(&subject)?;
    builder.set_pubkey(&key)?;
    builder.set_not_before(Asn1Time::days_from_now(0)?.as_ref())?;
    builder.set_not_after(Asn1Time::days_from_now(3650)?.as_ref())?;
    builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
    builder.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()?,
    )?;
    builder.sign(&key, MessageDigest::sha256())?;
    let cert = builder.build();
    Ok((cert.to_pem()?, key.private_key_to_pem_pkcs8()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer(options: SignerOptions) -> CaSigner {
        let (cert, key) = generate_test_ca("AgentENV Egress Test CA").unwrap();
        CaSigner::from_pem(&cert, &key, options).unwrap()
    }

    #[test]
    fn a_leaf_is_minted_once_and_then_served_from_the_cache() {
        let signer = signer(SignerOptions::default());
        let first = signer.leaf_for("api.example.com", "sbx-1").unwrap();
        let second = signer.leaf_for("api.example.com", "sbx-2").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.name, "api.example.com");
        assert!(first.expires_at > SystemTime::now());
    }

    #[test]
    fn names_no_certificate_can_carry_are_refused_before_any_signing() {
        let signer = signer(SignerOptions::default());
        for bad in [
            "",
            "*.example.com",
            "-bad.example",
            "a..b",
            "with space.example",
        ] {
            assert!(
                matches!(signer.leaf_for(bad, "sbx"), Err(SignError::InvalidName(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_per_sandbox_minting_budget_is_enforced_per_minute() {
        let signer = signer(SignerOptions {
            mints_per_sandbox_per_minute: 2,
            ..SignerOptions::default()
        });
        signer.leaf_for("a.example", "sbx").unwrap();
        signer.leaf_for("b.example", "sbx").unwrap();
        assert!(matches!(
            signer.leaf_for("c.example", "sbx"),
            Err(SignError::RateLimited)
        ));
        // Another sandbox has its own budget, and a cached name costs nothing.
        signer.leaf_for("c.example", "other").unwrap();
        signer.leaf_for("a.example", "sbx").unwrap();
    }

    #[test]
    fn the_cache_evicts_its_oldest_name_at_capacity() {
        let signer = signer(SignerOptions {
            cache_capacity: 2,
            mints_per_sandbox_per_minute: 100,
            ..SignerOptions::default()
        });
        let a = signer.leaf_for("a.example", "sbx").unwrap();
        signer.leaf_for("b.example", "sbx").unwrap();
        signer.leaf_for("c.example", "sbx").unwrap();
        let a_again = signer.leaf_for("a.example", "sbx").unwrap();
        assert!(!Arc::ptr_eq(&a, &a_again), "a was evicted and minted again");
    }
}
