//! Where this broker's signing key comes from: the api half issues one
//! intermediate per node, and this keeps the current one in the slot the
//! handler mints from.
//!
//! A broker that cannot reach the issuer does not refuse to start. It serves
//! the passthrough path and closes matched names, which is a failure an
//! operator sees on one node rather than a node that never comes up.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tracing::{info, warn};

use crate::tls::{CaSigner, SignerOptions, SignerSlot};

/// Path the intermediate is issued at, appended to the same base
/// `[resolver].url` names.
pub const INTERMEDIATE_PATH: &str = "egress/intermediate";

/// How often the current intermediate's remaining life is looked at.
pub const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Below this, an operator is told on every check: an intermediate this close
/// to expiry means renewal has been failing for days.
pub const EXPIRY_WARNING: Duration = Duration::from_secs(24 * 3600);

/// The renewal reason that also empties the slot.
const EXPIRED: &str = "expired";

/// The api half's issuing endpoint.
pub struct IntermediateIssuer {
    client: reqwest::Client,
    url: reqwest::Url,
    token_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssuedBody {
    certificate: String,
    key: String,
    root: String,
}

impl IntermediateIssuer {
    /// `base` is a directory URL, the same one credentials resolve against.
    pub fn new(base: &str, token_file: PathBuf, timeout: Duration) -> anyhow::Result<Self> {
        let base = base.trim();
        let base = reqwest::Url::parse(&if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{base}/")
        })?;
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            url: base.join(INTERMEDIATE_PATH)?,
            token_file,
        })
    }

    /// Asks for this node's intermediate and builds a signer from it.
    ///
    /// The token is read on every call: a projected ServiceAccount token is
    /// rotated in place, and one read at startup would expire in an hour.
    pub async fn issue(&self, options: SignerOptions) -> anyhow::Result<CaSigner> {
        let token = std::fs::read_to_string(&self.token_file)
            .map_err(|err| anyhow::anyhow!("read {:?}: {err}", self.token_file))?;
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(token.trim())
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("the issuer answered {status}");
        }
        let body: IssuedBody = response.json().await?;
        // The root is what guests trust; a broker serving a chain that does
        // not end at it would hand every guest an untrusted leaf.
        anyhow::ensure!(
            body.root.contains("BEGIN CERTIFICATE"),
            "the issuer returned no root certificate"
        );
        CaSigner::from_pem(body.certificate.as_bytes(), body.key.as_bytes(), options)
            .map_err(|err| anyhow::anyhow!("the issued intermediate is unusable: {err}"))
    }
}

/// Keeps `slot` holding a usable intermediate, renewing at a third of its
/// life remaining and publishing how long that is.
pub async fn keep_current(
    slot: Arc<SignerSlot>,
    issuer: Arc<IntermediateIssuer>,
    options: SignerOptions,
) {
    loop {
        if let Some(reason) = renewal_reason(slot.load().as_deref(), SystemTime::now()) {
            if reason == EXPIRED {
                // Leaves under an expired issuer are leaves no guest accepts,
                // and minting them turns a closed name into a handshake that
                // fails halfway. Empty the slot and answer the way a broker
                // that was never issued one answers: closed.
                slot.clear();
                warn!("this node's egress intermediate expired; rules domains are closed");
            }
            match issuer.issue(options.clone()).await {
                Ok(signer) => {
                    let not_after = signer.not_after();
                    slot.store(Arc::new(signer));
                    info!(reason, ?not_after, "took a new egress intermediate");
                }
                Err(err) => warn!(
                    reason,
                    error = %format_args!("{err:#}"),
                    "could not take an egress intermediate; rules domains stay unbrokered"
                ),
            }
        }
        publish_remaining(slot.load().as_deref(), SystemTime::now());
        tokio::time::sleep(RENEWAL_CHECK_INTERVAL).await;
    }
}

/// Why the intermediate should be replaced now, or `None` to keep it.
fn renewal_reason(signer: Option<&CaSigner>, now: SystemTime) -> Option<&'static str> {
    let Some(signer) = signer else {
        return Some("none held");
    };
    let remaining = remaining(signer, now);
    if remaining.is_zero() {
        return Some(EXPIRED);
    }
    // A third of the window it was signed for: two renewal failures in a row
    // still leave days before anything stops working.
    (remaining * 3 <= signer.lifetime()).then_some("a third of its life left")
}

fn remaining(signer: &CaSigner, now: SystemTime) -> Duration {
    signer
        .not_after()
        .duration_since(now)
        .unwrap_or(Duration::ZERO)
}

fn publish_remaining(signer: Option<&CaSigner>, now: SystemTime) {
    let remaining = signer.map_or(Duration::ZERO, |signer| remaining(signer, now));
    metrics::gauge!("egress_intermediate_expires_seconds").set(remaining.as_secs_f64());
    if remaining < EXPIRY_WARNING {
        warn!(
            remaining_secs = remaining.as_secs(),
            "this node's egress intermediate is close to expiry; renewal has been failing"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::generate_test_ca;

    fn signer() -> CaSigner {
        let (cert, key) = generate_test_ca("issuer test").unwrap();
        CaSigner::from_pem(&cert, &key, SignerOptions::default()).unwrap()
    }

    #[test]
    fn an_expired_intermediate_leaves_the_slot_empty_rather_than_minting_under_it() {
        let slot = SignerSlot::holding(Arc::new(signer()));
        let held = slot.load().expect("a signer is held");
        let after_expiry = held.not_after() + Duration::from_secs(1);

        assert_eq!(
            renewal_reason(Some(&held), after_expiry),
            Some(EXPIRED),
            "the reason that empties the slot has to be the one this checks for"
        );
        slot.clear();

        assert!(
            slot.load().is_none(),
            "a leaf under an expired issuer is one no guest accepts"
        );
    }

    #[test]
    fn an_empty_slot_is_always_a_reason_to_ask() {
        assert_eq!(renewal_reason(None, SystemTime::now()), Some("none held"));
    }

    #[test]
    fn a_fresh_intermediate_is_kept_and_one_past_two_thirds_is_replaced() {
        let signer = signer();
        let now = SystemTime::now();

        assert_eq!(renewal_reason(Some(&signer), now), None);

        // `generate_test_ca` signs for ten years; two thirds through it is
        // a renewal, and past `not_after` it is an expiry.
        let two_thirds = now + signer.lifetime() * 2 / 3 + Duration::from_secs(60);
        assert_eq!(
            renewal_reason(Some(&signer), two_thirds),
            Some("a third of its life left")
        );
        assert_eq!(
            renewal_reason(
                Some(&signer),
                now + signer.lifetime() + Duration::from_secs(60)
            ),
            Some("expired")
        );
    }

    #[test]
    fn a_base_url_with_a_path_prefix_keeps_that_prefix() {
        let issuer = IntermediateIssuer::new(
            "http://agentenv-api:8000/internal",
            PathBuf::from("/var/run/secrets/aenv/api/token"),
            Duration::from_secs(5),
        )
        .unwrap();

        assert_eq!(
            issuer.url.as_str(),
            "http://agentenv-api:8000/internal/egress/intermediate"
        );
    }
}
