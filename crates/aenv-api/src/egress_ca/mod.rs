//! Short-lived intermediate CAs, one per node, signed by the root the guests
//! trust.
//!
//! Guests trust the **root**, never a node's intermediate: a sandbox pauses on
//! one node and resumes on another, and a process that loaded its trust store
//! once — Node.js, Go, the JVM — carries the store it had across the move. A
//! guest pinned to the node it started on would keep working until it moved
//! and then fail with no way to tell why.
//!
//! What the intermediate buys is blast radius: a broker holds a key that
//! expires in a week and cannot sign a name inside the cluster, and the root
//! key never leaves this half.

mod name_constraints;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::x509::extension::{BasicConstraints, KeyUsage, SubjectKeyIdentifier};
use openssl::x509::{X509Builder, X509NameBuilder, X509};

pub use name_constraints::{name_constraints_value, EXCLUDED_DOMAINS, EXCLUDED_NETWORKS};

/// How long a node's intermediate is good for. The broker renews at a third
/// of it remaining, so a node that cannot reach this half has days, not
/// minutes, to be noticed.
pub const INTERMEDIATE_LIFETIME: Duration = Duration::from_secs(7 * 24 * 3600);

/// One issued intermediate: the certificate chain a broker serves and the key
/// it signs leaves with. The key never touches this half's disk or logs.
pub struct NodeIntermediate {
    pub certificate_pem: Vec<u8>,
    pub key_pem: zeroize::Zeroizing<Vec<u8>>,
    /// The root, so a broker can serve the whole chain without holding a
    /// second copy of it.
    pub root_pem: Vec<u8>,
    pub not_after_unix: i64,
}

/// The root this half signs with. Loaded once at startup from the `egress-ca`
/// Secret; the key is what makes this half the only issuer.
pub struct EgressRootCa {
    key: PKey<Private>,
    certificate: X509,
    certificate_pem: Vec<u8>,
}

impl EgressRootCa {
    pub fn from_pem(certificate_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let certificate =
            X509::from_pem(certificate_pem).context("the egress root certificate is not PEM")?;
        let key =
            PKey::private_key_from_pem(key_pem).context("the egress root key is not PKCS#8 PEM")?;
        Ok(Self {
            key,
            certificate,
            certificate_pem: certificate_pem.to_vec(),
        })
    }

    pub fn certificate_pem(&self) -> &[u8] {
        &self.certificate_pem
    }

    /// Issues `node_id` an intermediate valid for [`INTERMEDIATE_LIFETIME`].
    ///
    /// `pathlen:0` so it can sign leaves and nothing that signs further, and
    /// an excluded-set `nameConstraints` so no leaf under it can carry a
    /// cluster-internal name or address whatever the broker is asked for.
    pub fn issue_node_intermediate(&self, node_id: &str) -> Result<NodeIntermediate> {
        let now = SystemTime::now();
        self.issue_at(node_id, now, INTERMEDIATE_LIFETIME)
    }

    fn issue_at(
        &self,
        node_id: &str,
        now: SystemTime,
        lifetime: Duration,
    ) -> Result<NodeIntermediate> {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
        let key = PKey::from_ec_key(EcKey::generate(&group)?)?;

        let mut subject = X509NameBuilder::new()?;
        subject.append_entry_by_nid(Nid::COMMONNAME, &format!("AgentENV Egress Node {node_id}"))?;
        let subject = subject.build();

        let mut builder = X509Builder::new()?;
        builder.set_version(2)?;
        let mut serial = BigNum::new()?;
        serial.rand(127, MsbOption::MAYBE_ZERO, false)?;
        builder.set_serial_number(serial.to_asn1_integer()?.as_ref())?;
        builder.set_subject_name(&subject)?;
        builder.set_issuer_name(self.certificate.subject_name())?;
        builder.set_pubkey(&key)?;
        // A clock a few minutes behind this half must not reject a fresh
        // intermediate outright.
        let not_before = unix_secs(now).saturating_sub(300);
        let not_after = unix_secs(now + lifetime);
        builder.set_not_before(Asn1Time::from_unix(not_before)?.as_ref())?;
        builder.set_not_after(Asn1Time::from_unix(not_after)?.as_ref())?;
        builder.append_extension(BasicConstraints::new().critical().ca().pathlen(0).build()?)?;
        builder.append_extension(
            KeyUsage::new()
                .critical()
                .key_cert_sign()
                .crl_sign()
                .build()?,
        )?;
        let context = builder.x509v3_context(Some(&self.certificate), None);
        let subject_key_id = SubjectKeyIdentifier::new().build(&context)?;
        let constraints = name_constraints::extension(&context)?;
        builder.append_extension(subject_key_id)?;
        builder.append_extension(constraints)?;
        builder.sign(&self.key, MessageDigest::sha256())?;
        let certificate = builder.build();

        Ok(NodeIntermediate {
            certificate_pem: certificate.to_pem()?,
            key_pem: zeroize::Zeroizing::new(key.private_key_to_pem_pkcs8()?),
            root_pem: self.certificate_pem.clone(),
            not_after_unix: not_after,
        })
    }
}

fn unix_secs(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// A self-signed root for tests and for a local run: (certificate, key) PEM.
pub fn generate_root(common_name: &str) -> Result<(Vec<u8>, Vec<u8>)> {
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
    let certificate = builder.build();
    Ok((certificate.to_pem()?, key.private_key_to_pem_pkcs8()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> EgressRootCa {
        let (certificate, key) = generate_root("AgentENV Egress Test Root").unwrap();
        EgressRootCa::from_pem(&certificate, &key).unwrap()
    }

    fn text_of(certificate: &X509) -> String {
        String::from_utf8(certificate.to_text().unwrap()).unwrap()
    }

    #[test]
    fn an_intermediate_can_sign_leaves_and_nothing_below_them() {
        let issued = root().issue_node_intermediate("node-a").unwrap();
        let certificate = X509::from_pem(&issued.certificate_pem).unwrap();
        let text = text_of(&certificate);

        assert!(text.contains("CA:TRUE"), "{text}");
        assert!(text.contains("pathlen:0"), "{text}");
        assert!(text.contains("Certificate Sign"), "{text}");
        assert!(
            text.contains("AgentENV Egress Node node-a"),
            "the subject names the node: {text}"
        );
    }

    #[test]
    fn an_intermediate_excludes_every_cluster_internal_name_and_range() {
        let issued = root().issue_node_intermediate("node-a").unwrap();
        let text = text_of(&X509::from_pem(&issued.certificate_pem).unwrap());

        assert!(text.contains("X509v3 Name Constraints"), "{text}");
        for domain in EXCLUDED_DOMAINS {
            assert!(text.contains(domain), "{domain} is not excluded: {text}");
        }
        for (network, _) in EXCLUDED_NETWORKS {
            assert!(text.contains(network), "{network} is not excluded: {text}");
        }
        assert!(
            !text.contains("Permitted"),
            "an excluded set only; a permitted one would refuse every public name: {text}"
        );
    }

    #[test]
    fn an_intermediate_is_signed_by_the_root_and_expires_in_a_week() {
        let root = root();
        let issued = root.issue_node_intermediate("node-a").unwrap();
        let certificate = X509::from_pem(&issued.certificate_pem).unwrap();
        let root_certificate = X509::from_pem(&issued.root_pem).unwrap();

        assert!(certificate
            .verify(&root_certificate.public_key().unwrap())
            .unwrap());
        let lifetime = issued.not_after_unix - unix_secs(SystemTime::now());
        assert!(
            (INTERMEDIATE_LIFETIME.as_secs() as i64 - lifetime).abs() < 60,
            "issued for {lifetime}s"
        );
    }

    #[test]
    fn two_nodes_get_different_keys_and_different_serials() {
        let root = root();
        let a = root.issue_node_intermediate("node-a").unwrap();
        let b = root.issue_node_intermediate("node-b").unwrap();

        assert_ne!(a.key_pem.as_slice(), b.key_pem.as_slice());
        assert_ne!(
            X509::from_pem(&a.certificate_pem)
                .unwrap()
                .serial_number()
                .to_bn()
                .unwrap()
                .to_vec(),
            X509::from_pem(&b.certificate_pem)
                .unwrap()
                .serial_number()
                .to_bn()
                .unwrap()
                .to_vec()
        );
    }

    #[test]
    fn a_root_that_is_not_a_pem_pair_is_refused() {
        let (certificate, key) = generate_root("AgentENV Egress Test Root").unwrap();

        assert!(EgressRootCa::from_pem(b"not a certificate", &key).is_err());
        assert!(EgressRootCa::from_pem(&certificate, b"not a key").is_err());
    }
}
