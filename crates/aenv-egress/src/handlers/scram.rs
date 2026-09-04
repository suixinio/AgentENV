//! SCRAM-SHA-256 (RFC 5802/7677) as a client, for authenticating to an
//! upstream with a credential the guest never sees.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

const KEY_LEN: usize = 32;
/// Iteration counts above this are refused: a hostile or misconfigured server
/// could otherwise spend the broker's CPU on one connection.
pub const MAX_ITERATIONS: u32 = 1_000_000;

#[derive(Debug, thiserror::Error)]
pub enum ScramError {
    #[error("the server's {0} message is malformed")]
    Malformed(&'static str),
    #[error("the server did not echo the client nonce")]
    NonceMismatch,
    #[error("the server asked for {0} iterations, more than {MAX_ITERATIONS}")]
    TooManyIterations(u32),
    #[error("the server's signature does not verify")]
    BadServerSignature,
}

/// One SCRAM-SHA-256 exchange in progress. Holds the salted password until
/// the server signature is verified, then drops it.
pub struct ScramClient {
    client_first_bare: String,
    client_nonce: String,
    password: Zeroizing<String>,
    salted: Option<Zeroizing<[u8; KEY_LEN]>>,
    auth_message: Option<String>,
}

impl ScramClient {
    /// `user` is carried in the message for interoperability only: a Postgres
    /// server authenticates the user from the startup packet, not from here.
    pub fn new(user: &str, password: &str, client_nonce: String) -> Self {
        let client_first_bare = format!("n={},r={}", saslprep_lite(user), client_nonce);
        Self {
            client_first_bare,
            client_nonce,
            password: Zeroizing::new(password.to_string()),
            salted: None,
            auth_message: None,
        }
    }

    /// `n,,` plus the bare message: the channel binding this client does not
    /// use, which is what `c=biws` later confirms.
    pub fn client_first(&self) -> String {
        format!("n,,{}", self.client_first_bare)
    }

    pub fn client_final(&mut self, server_first: &str) -> Result<String, ScramError> {
        let (nonce, salt, iterations) = parse_server_first(server_first)?;
        if !nonce.starts_with(&self.client_nonce) || nonce == self.client_nonce {
            return Err(ScramError::NonceMismatch);
        }
        if iterations > MAX_ITERATIONS || iterations == 0 {
            return Err(ScramError::TooManyIterations(iterations));
        }
        let salted = Zeroizing::new(pbkdf2_sha256(self.password.as_bytes(), &salt, iterations));
        let client_key = hmac(&salted[..], b"Client Key");
        let stored_key: [u8; KEY_LEN] = Sha256::digest(client_key).into();

        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, without_proof
        );
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(key, signature)| key ^ signature)
            .collect();

        self.salted = Some(salted);
        self.auth_message = Some(auth_message);
        Ok(format!(
            "{without_proof},p={}",
            base64::engine::general_purpose::STANDARD.encode(proof)
        ))
    }

    pub fn verify_server_final(&mut self, server_final: &str) -> Result<(), ScramError> {
        let (salted, auth_message) = match (self.salted.take(), self.auth_message.take()) {
            (Some(salted), Some(auth_message)) => (salted, auth_message),
            _ => return Err(ScramError::Malformed("final")),
        };
        let verifier = server_final
            .split(',')
            .find_map(|part| part.strip_prefix("v="))
            .ok_or(ScramError::Malformed("final"))?;
        let verifier = base64::engine::general_purpose::STANDARD
            .decode(verifier)
            .map_err(|_| ScramError::Malformed("final"))?;
        let server_key = hmac(&salted[..], b"Server Key");
        let expected = hmac(&server_key, auth_message.as_bytes());
        if verifier.len() != expected.len() || !constant_time_eq(&verifier, &expected) {
            return Err(ScramError::BadServerSignature);
        }
        Ok(())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (l, r)| acc | (l ^ r))
        == 0
}

fn parse_server_first(message: &str) -> Result<(String, Vec<u8>, u32), ScramError> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    for part in message.split(',') {
        match part.split_at_checked(2) {
            Some(("r=", value)) => nonce = Some(value.to_string()),
            Some(("s=", value)) => {
                salt = base64::engine::general_purpose::STANDARD.decode(value).ok()
            }
            Some(("i=", value)) => iterations = value.parse::<u32>().ok(),
            _ => {}
        }
    }
    match (nonce, salt, iterations) {
        (Some(nonce), Some(salt), Some(iterations)) => Ok((nonce, salt, iterations)),
        _ => Err(ScramError::Malformed("first")),
    }
}

fn hmac(key: &[u8], message: &[u8]) -> [u8; KEY_LEN] {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// PBKDF2-HMAC-SHA256 with `dkLen` equal to one hash block, which is the only
/// length SCRAM-SHA-256 uses.
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; KEY_LEN] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut current = hmac(password, &block);
    let mut result = current;
    for _ in 1..iterations {
        current = hmac(password, &current);
        for (out, next) in result.iter_mut().zip(current.iter()) {
            *out ^= next;
        }
    }
    result
}

/// The subset of SASLprep a Postgres client needs: `=` and `,` are the
/// message separators and must be escaped so a user name cannot forge a
/// message attribute.
fn saslprep_lite(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pbkdf2_matches_the_rfc_7677_vector() {
        // RFC 7677 §3: password "pencil", salt W22ZaJ0SNY7soEsUEjb6gQ==, i=4096.
        let salt = base64::engine::general_purpose::STANDARD
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .unwrap();
        let salted = pbkdf2_sha256(b"pencil", &salt, 4096);
        let client_key = hmac(&salted, b"Client Key");
        let stored: [u8; 32] = Sha256::digest(client_key).into();
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(stored),
            "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY="
        );
    }

    #[test]
    fn the_rfc_7677_exchange_produces_its_stated_proof_and_verifier() {
        let mut client = ScramClient::new("user", "pencil", "rOprNGfwEbeRWgbNEkqO".into());
        assert_eq!(client.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

        let server_first =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let final_message = client.client_final(server_first).unwrap();
        assert_eq!(
            final_message,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        client
            .verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn a_server_that_does_not_extend_the_client_nonce_is_refused() {
        let mut client = ScramClient::new("user", "pencil", "clientnonce".into());
        for server_first in [
            "r=othernonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
            "r=clientnonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
        ] {
            assert!(matches!(
                client.client_final(server_first),
                Err(ScramError::NonceMismatch)
            ));
        }
    }

    #[test]
    fn a_malformed_or_extravagant_server_first_is_refused() {
        let mut client = ScramClient::new("user", "pencil", "clientnonce".into());
        assert!(matches!(
            client.client_final("s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"),
            Err(ScramError::Malformed("first"))
        ));
        assert!(matches!(
            client.client_final("r=clientnonceX,s=notbase64!,i=4096"),
            Err(ScramError::Malformed("first"))
        ));
        assert!(matches!(
            client.client_final("r=clientnonceX,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=99999999"),
            Err(ScramError::TooManyIterations(_))
        ));
        assert!(matches!(
            client.client_final("r=clientnonceX,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=0"),
            Err(ScramError::TooManyIterations(0))
        ));
    }

    #[test]
    fn a_wrong_server_signature_is_refused_and_a_verified_one_clears_the_salted_password() {
        let mut client = ScramClient::new("user", "pencil", "rOprNGfwEbeRWgbNEkqO".into());
        client
            .client_final(
                "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
            )
            .unwrap();
        assert!(client.salted.is_some());
        assert!(matches!(
            client.verify_server_final("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            Err(ScramError::BadServerSignature)
        ));
        assert!(
            client.salted.is_none(),
            "the salted password does not outlive the check"
        );
        assert!(matches!(
            client.verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
            Err(ScramError::Malformed("final"))
        ));
    }

    #[test]
    fn a_user_name_cannot_forge_a_message_attribute() {
        let client = ScramClient::new("bob,r=evil", "p", "nonce".into());
        assert_eq!(client.client_first(), "n,,n=bob=2Cr=3Devil,r=nonce");
    }
}
