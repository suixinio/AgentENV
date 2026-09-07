//! Leaf certificates for intercepted names, signed by the root the guests
//! trust — the same key every broker in a deployment holds. A leaf is minted
//! only after a rule matched the name; a name no rule covers is never signed.
//!
//! One layer, and no chain on the wire: the guest already has the certificate
//! that signed the leaf, because `[egress_broker].guest_ca_cert_path` put it
//! in the trust store before any of this ran.

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
    not_after: SystemTime,
    options: SignerOptions,
    cache: Mutex<LeafCache>,
    budgets: Mutex<HashMap<String, (u64, u32)>>,
}

/// How long a leaf must still outlive the root that signed it. A leaf that
/// outlived its issuer would fail verification for the rest of its own life,
/// and the cache would keep serving it.
const ISSUER_MARGIN: Duration = Duration::from_secs(3600);

/// A leaf still worth serving has to outlive the connection about to use it.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);

struct LeafCache {
    by_name: HashMap<String, CachedLeaf>,
    /// Insertion order for eviction. A refreshed name leaves its previous
    /// entry behind, so an entry only evicts the leaf whose `seq` it names.
    order: VecDeque<(String, u64)>,
    next_seq: u64,
}

struct CachedLeaf {
    leaf: Arc<Leaf>,
    seq: u64,
}

impl CaSigner {
    pub fn from_pem(
        ca_cert_pem: &[u8],
        ca_key_pem: &[u8],
        options: SignerOptions,
    ) -> Result<Self, SignError> {
        let cert = X509::from_pem(ca_cert_pem)?;
        let key = PKey::private_key_from_pem(ca_key_pem)?;
        let not_after = asn1_to_system_time(cert.not_after())?;
        Ok(Self {
            key,
            cert,
            not_after,
            options,
            cache: Mutex::new(LeafCache {
                by_name: HashMap::new(),
                order: VecDeque::new(),
                next_seq: 0,
            }),
            budgets: Mutex::new(HashMap::new()),
        })
    }

    /// When the root this signs with stops being usable, which is also when
    /// the leaves it has already minted stop verifying.
    pub fn not_after(&self) -> SystemTime {
        self.not_after
    }

    /// A leaf for `name`, from the cache when one is live, otherwise minted
    /// against `sandbox_id`'s per-minute budget. The cache lock is held
    /// across minting, so concurrent connections for one name mint once and
    /// charge the budget once instead of racing each other.
    pub fn leaf_for(&self, name: &str, sandbox_id: &str) -> Result<Arc<Leaf>, SignError> {
        if !is_certifiable_name(name) {
            return Err(SignError::InvalidName(name.to_string()));
        }
        let now = SystemTime::now();
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = cache.by_name.get(name) {
            if cached.leaf.expires_at > now + REFRESH_MARGIN {
                return Ok(Arc::clone(&cached.leaf));
            }
            cache.by_name.remove(name);
        }
        self.charge_budget(sandbox_id, now)?;
        let leaf = Arc::new(self.mint(name, now)?);
        #[cfg(feature = "tls")]
        metrics::counter!("egress_tls_leaf_minted_total").increment(1);
        cache.next_seq += 1;
        let seq = cache.next_seq;
        while cache.by_name.len() >= self.options.cache_capacity {
            let Some((oldest, oldest_seq)) = cache.order.pop_front() else {
                break;
            };
            if cache
                .by_name
                .get(&oldest)
                .is_some_and(|cached| cached.seq == oldest_seq)
            {
                cache.by_name.remove(&oldest);
            }
        }
        cache.by_name.insert(
            name.to_string(),
            CachedLeaf {
                leaf: Arc::clone(&leaf),
                seq,
            },
        );
        cache.order.push_back((name.to_string(), seq));
        Ok(leaf)
    }

    /// A leaf lives for its TTL, or until an hour before the root stops being
    /// usable, whichever comes first. The root outlasts a leaf by years in
    /// every normal deployment; this is what keeps the last day before a root
    /// expires from minting leaves nothing can verify.
    fn leaf_expiry(&self, now: SystemTime) -> SystemTime {
        let by_ttl = now + self.options.leaf_ttl;
        let by_issuer = self.not_after.checked_sub(ISSUER_MARGIN).unwrap_or(now);
        by_ttl.min(by_issuer).max(now)
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
        let (leaf_cert, leaf_key, expires_at) = self.mint_cert(name, now)?;

        // The leaf alone: what signed it is the root already in the guest's
        // trust store, and sending a certificate the peer must have anyway
        // adds a round of bytes and nothing else.
        let leaf_pem = leaf_cert.to_pem()?;
        let key_pem = leaf_key.private_key_to_pem_pkcs8()?;
        let identity = native_tls::Identity::from_pkcs8(&leaf_pem, &key_pem)?;
        let acceptor = native_tls::TlsAcceptor::builder(identity)
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()?;
        Ok(Leaf {
            name: name.to_string(),
            acceptor: tokio_native_tls::TlsAcceptor::from(acceptor),
            expires_at,
        })
    }

    fn mint_cert(
        &self,
        name: &str,
        now: SystemTime,
    ) -> Result<(X509, PKey<Private>, SystemTime), SignError> {
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
        let expires_at = self.leaf_expiry(now);
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
        Ok((builder.build(), leaf_key, expires_at))
    }
}

/// An X.509 timestamp as a `SystemTime`, by asking OpenSSL for its distance
/// from the epoch — the only portable way out of an `Asn1Time`.
fn asn1_to_system_time(at: &openssl::asn1::Asn1TimeRef) -> Result<SystemTime, SignError> {
    let epoch = Asn1Time::from_unix(0)?;
    let difference = epoch.diff(at)?;
    let seconds = i64::from(difference.days) * 86_400 + i64::from(difference.secs);
    Ok(if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    })
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

/// A self-signed CA certificate and its key, good for `days`.
fn new_root(common_name: &str, days: u32) -> Result<(X509, PKey<Private>), SignError> {
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
    builder.set_not_after(Asn1Time::days_from_now(days)?.as_ref())?;
    builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
    builder.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()?,
    )?;
    builder.sign(&key, MessageDigest::sha256())?;
    Ok((builder.build(), key))
}

/// A self-signed root, as (cert PEM, PKCS#8 key PEM). The one a deployment
/// with no Secret to mount mints for itself, and the one tests use.
pub fn generate_root(common_name: &str) -> Result<(Vec<u8>, Vec<u8>), SignError> {
    let (cert, key) = new_root(common_name, 3650)?;
    Ok((cert.to_pem()?, key.private_key_to_pem_pkcs8()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_leaf_never_outlives_the_issuer_that_signed_it() {
        let (cert, key) = generate_root("issuer margin").unwrap();
        // A leaf TTL longer than the issuer's own remaining life.
        let signer = CaSigner::from_pem(
            &cert,
            &key,
            SignerOptions {
                leaf_ttl: Duration::from_secs(100 * 365 * 24 * 3600),
                ..SignerOptions::default()
            },
        )
        .unwrap();

        let leaf = signer.leaf_for("api.test", "sbx-1").unwrap();

        assert!(
            leaf.expires_at <= signer.not_after() - ISSUER_MARGIN,
            "a leaf outliving its issuer would fail verification for the rest of its life"
        );
    }

    #[test]
    fn a_leaf_shorter_than_its_issuer_keeps_its_own_ttl() {
        let (cert, key) = generate_root("issuer margin").unwrap();
        let leaf_ttl = Duration::from_secs(3600);
        let signer = CaSigner::from_pem(
            &cert,
            &key,
            SignerOptions {
                leaf_ttl,
                ..SignerOptions::default()
            },
        )
        .unwrap();

        let leaf = signer.leaf_for("api.test", "sbx-1").unwrap();
        let now = SystemTime::now();

        let lived = leaf.expires_at.duration_since(now).unwrap();
        assert!(
            lived.abs_diff(leaf_ttl) < Duration::from_secs(60),
            "{lived:?} is not the configured {leaf_ttl:?}"
        );
    }

    #[test]
    fn the_leaf_is_signed_by_the_root_the_guest_was_given() {
        let (cert, key) = generate_root("single layer root").unwrap();
        let signer = CaSigner::from_pem(&cert, &key, SignerOptions::default()).unwrap();

        let (leaf, _, _) = signer.mint_cert("api.test", SystemTime::now()).unwrap();
        let root = X509::from_pem(&cert).unwrap();

        assert!(
            leaf.verify(root.public_key().unwrap().as_ref()).unwrap(),
            "nothing between the root and the leaf: the guest trusts the root alone"
        );
        assert_eq!(
            leaf.issuer_name().try_cmp(root.subject_name()).unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn a_generated_root_signs_itself() {
        let (cert, _) = generate_root("gen-ca root").unwrap();
        let root = X509::from_pem(&cert).unwrap();

        assert!(root.verify(root.public_key().unwrap().as_ref()).unwrap());
        assert_eq!(
            root.subject_name().try_cmp(root.issuer_name()).unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    fn signer(options: SignerOptions) -> CaSigner {
        let (cert, key) = generate_root("AgentENV Egress Test CA").unwrap();
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

    /// Refreshing a name leaves its earlier queue entry at the front; that
    /// entry must not evict the leaf now standing under the same name.
    #[test]
    fn a_refreshed_name_is_not_evicted_by_the_queue_entry_it_replaced() {
        let signer = signer(SignerOptions {
            cache_capacity: 3,
            mints_per_sandbox_per_minute: 100,
            ..SignerOptions::default()
        });
        let first = signer.leaf_for("a.example", "sbx").unwrap();
        signer.leaf_for("b.example", "sbx").unwrap();
        // What an expiry does: the entry leaves the map and its queue entry
        // stays behind, so the refresh below appends a second one.
        signer.cache.lock().unwrap().by_name.remove("a.example");
        let refreshed = signer.leaf_for("a.example", "sbx").unwrap();
        assert!(!Arc::ptr_eq(&first, &refreshed));

        signer.leaf_for("c.example", "sbx").unwrap();
        // Eviction now runs and meets a's stale queue entry before b's.
        signer.leaf_for("d.example", "sbx").unwrap();

        let again = signer.leaf_for("a.example", "sbx").unwrap();
        assert!(
            Arc::ptr_eq(&refreshed, &again),
            "the stale queue entry evicted the leaf that replaced it"
        );
    }

    #[test]
    fn concurrent_connections_for_one_name_mint_once_and_charge_the_budget_once() {
        let signer = Arc::new(signer(SignerOptions {
            mints_per_sandbox_per_minute: 1,
            ..SignerOptions::default()
        }));
        let leaves: Vec<Arc<Leaf>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let signer = Arc::clone(&signer);
                    scope.spawn(move || signer.leaf_for("api.example.com", "sbx-1"))
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap()
                        .expect("one mint is within a budget of one")
                })
                .collect()
        });
        assert!(
            leaves
                .windows(2)
                .all(|pair| Arc::ptr_eq(&pair[0], &pair[1])),
            "every caller must get the one leaf that was minted"
        );
    }
}
