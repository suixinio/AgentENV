//! The `secret_values` and `secret_grants` tables: encrypted values and the
//! grants that make them readable.
//!
//! [`SecretsBackend`] has no read operation, so `/secrets` cannot reach a
//! plaintext through this type at all. [`PgSecretValues::resolve`] is the one
//! method that opens an envelope, and the internal resolve endpoint is its
//! only caller.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use aenv_core::secrets::{SecretValue, SecretsBackend, SecretsError};
use anyhow::anyhow;
use async_trait::async_trait;
use sqlx::{PgPool, Row};
use zeroize::Zeroizing;

use crate::secrets::envelope::{Envelope, Sealed};

const KIND_OPAQUE: &str = "opaque";
const KIND_FIELDS: &str = "fields";

pub struct PgSecretValues {
    pool: PgPool,
    envelope: Envelope,
}

/// Why a resolve did not produce a credential. `Denied` is everything the
/// caller is not entitled to *and* everything that does not exist: the
/// broker turns both into the guest's synthetic 403, and telling them apart
/// would tell a caller which names exist.
#[derive(Debug)]
pub enum ResolveError {
    Denied,
    Unavailable(anyhow::Error),
}

/// One resolved credential, in the shape the broker's resolver contract
/// expects. Both forms carry the pin stored with the version they came from.
pub enum ResolvedCredential {
    Opaque {
        value: Zeroizing<String>,
        allowed_hosts: Vec<String>,
    },
    Fields {
        fields: BTreeMap<String, Zeroizing<String>>,
        allowed_hosts: Vec<String>,
    },
}

impl PgSecretValues {
    pub fn new(pool: PgPool, envelope: Envelope) -> Self {
        Self { pool, envelope }
    }

    /// The credential `(sandbox_id, execution_id)` may read under `name`, or
    /// `Denied` when no grant covers it and when no version exists.
    pub async fn resolve(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<ResolvedCredential, ResolveError> {
        let granted: Option<(String, Vec<String>)> =
            sqlx::query("SELECT sandbox_id, names FROM secret_grants WHERE execution_id = $1")
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| ResolveError::Unavailable(err.into()))?
                .map(|row| (row.get("sandbox_id"), row.get("names")));

        let Some((granted_sandbox, names)) = granted else {
            return Err(ResolveError::Denied);
        };
        if granted_sandbox != sandbox_id || !names.iter().any(|granted| granted == name) {
            return Err(ResolveError::Denied);
        }

        let row = sqlx::query(
            "SELECT version, kind, ciphertext, nonce, allowed_hosts FROM secret_values \
             WHERE name = $1 ORDER BY version DESC LIMIT 1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| ResolveError::Unavailable(err.into()))?;
        let Some(row) = row else {
            return Err(ResolveError::Denied);
        };

        let version: i64 = row.get("version");
        let kind: String = row.get("kind");
        let allowed_hosts: Vec<String> = row.get("allowed_hosts");
        let sealed = Sealed {
            ciphertext: row.get("ciphertext"),
            nonce: row.get("nonce"),
        };
        let plaintext = self
            .envelope
            .open(name, version, &sealed)
            .map_err(ResolveError::Unavailable)?;

        match kind.as_str() {
            KIND_OPAQUE => Ok(ResolvedCredential::Opaque {
                value: decode_utf8(plaintext)?,
                allowed_hosts,
            }),
            KIND_FIELDS => {
                let fields: BTreeMap<String, String> = serde_json::from_slice(&plaintext)
                    .map_err(|err| ResolveError::Unavailable(err.into()))?;
                Ok(ResolvedCredential::Fields {
                    fields: fields
                        .into_iter()
                        .map(|(key, value)| (key, Zeroizing::new(value)))
                        .collect(),
                    allowed_hosts,
                })
            }
            other => Err(ResolveError::Unavailable(anyhow!(
                "stored value of {name:?} has kind {other:?}"
            ))),
        }
    }
}

fn decode_utf8(plaintext: Zeroizing<Vec<u8>>) -> Result<Zeroizing<String>, ResolveError> {
    match std::str::from_utf8(&plaintext) {
        Ok(text) => Ok(Zeroizing::new(text.to_string())),
        Err(_) => Err(ResolveError::Unavailable(anyhow!(
            "a stored value is not valid UTF-8"
        ))),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn unavailable(err: impl Into<anyhow::Error>) -> SecretsError {
    SecretsError::Unavailable(err.into())
}

fn plaintext_of(value: &SecretValue) -> Result<Zeroizing<Vec<u8>>, SecretsError> {
    match value {
        SecretValue::Opaque(value) => Ok(Zeroizing::new(value.expose().as_bytes().to_vec())),
        SecretValue::Fields(fields) => {
            let plain: BTreeMap<&str, &str> = fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.expose()))
                .collect();
            serde_json::to_vec(&plain)
                .map(Zeroizing::new)
                .map_err(unavailable)
        }
    }
}

#[async_trait]
impl SecretsBackend for PgSecretValues {
    /// Versions are allocated under a lock on the ref row rather than by
    /// `MAX(version) + 1` alone: the ciphertext is bound to the version it
    /// is stored at, so the number has to be settled before the seal.
    async fn put(
        &self,
        name: &str,
        value: &SecretValue,
        allowed_hosts: &[String],
    ) -> Result<i64, SecretsError> {
        let plaintext = plaintext_of(value)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;

        let locked = sqlx::query("SELECT 1 FROM secret_refs WHERE name = $1 FOR UPDATE")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        if locked.is_none() {
            return Err(SecretsError::NotFound);
        }

        let version: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM secret_values WHERE name = $1",
        )
        .bind(name)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;

        let sealed = self
            .envelope
            .seal(name, version, &plaintext)
            .map_err(unavailable)?;
        drop(plaintext);

        sqlx::query(
            "INSERT INTO secret_values \
             (name, version, kind, ciphertext, nonce, allowed_hosts, created_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(name)
        .bind(version)
        .bind(match value {
            SecretValue::Opaque(_) => KIND_OPAQUE,
            SecretValue::Fields(_) => KIND_FIELDS,
        })
        .bind(&sealed.ciphertext)
        .bind(&sealed.nonce)
        .bind(allowed_hosts)
        .bind(now_ms())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        tx.commit().await.map_err(unavailable)?;
        Ok(version)
    }

    async fn delete(&self, name: &str) -> Result<(), SecretsError> {
        sqlx::query("DELETE FROM secret_values WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    /// One row per execution, replaced wholesale: a regrant after a network
    /// update must not leave the names it dropped readable.
    async fn grant(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        names: &[String],
    ) -> Result<(), SecretsError> {
        sqlx::query(
            "INSERT INTO secret_grants (execution_id, sandbox_id, names, granted_at_ms) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (execution_id) DO UPDATE SET \
             sandbox_id = EXCLUDED.sandbox_id, names = EXCLUDED.names, \
             granted_at_ms = EXCLUDED.granted_at_ms",
        )
        .bind(execution_id)
        .bind(sandbox_id)
        .bind(names)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn revoke(&self, _sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError> {
        sqlx::query("DELETE FROM secret_grants WHERE execution_id = $1")
            .bind(execution_id)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    /// This backend owns the values, so a name with a ref row but no version
    /// is missing here: a policy naming it would be refused at resolve time
    /// whatever the ref store says.
    async fn missing_names(&self, names: &[String]) -> Result<Option<Vec<String>>, SecretsError> {
        let present: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT name FROM secret_values WHERE name = ANY($1)")
                .bind(names)
                .fetch_all(&self.pool)
                .await
                .map_err(unavailable)?;
        let present: BTreeSet<&str> = present.iter().map(String::as_str).collect();
        Ok(Some(
            names
                .iter()
                .filter(|name| !present.contains(name.as_str()))
                .cloned()
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use aenv_core::secrets::{SecretMetadata, SecretRefStore, SecretString};

    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::secrets::PgSecretRefStore;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;

    macro_rules! values_or_skip {
        ($name:literal) => {{
            let pool = isolated_schema_pool_or_skip!($name);
            migrate(&pool).await.expect("migration should succeed");
            (
                PgSecretValues::new(pool.clone(), Envelope::from_key_bytes(&[5u8; 32]).unwrap()),
                PgSecretRefStore::new(pool.clone()),
                pool,
            )
        }};
    }

    fn opaque(value: &str) -> SecretValue {
        SecretValue::Opaque(SecretString::new(value.to_string()))
    }

    fn fields(pairs: &[(&str, &str)]) -> SecretValue {
        SecretValue::Fields(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), SecretString::new(value.to_string())))
                .collect(),
        )
    }

    async fn named(refs: &PgSecretRefStore, name: &str) {
        refs.create(&format!("sec_{name}"), name, &SecretMetadata::new())
            .await
            .unwrap();
    }

    fn denied(result: Result<ResolvedCredential, ResolveError>) -> bool {
        matches!(result, Err(ResolveError::Denied))
    }

    #[tokio::test]
    async fn a_granted_opaque_value_resolves_and_carries_its_pin() {
        let (values, refs, _pool) = values_or_skip!("secret_values_opaque");
        named(&refs, "openai").await;

        let version = values
            .put(
                "openai",
                &opaque("sk-live"),
                &["api.openai.com".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(version, 1);
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        match values.resolve("sbx-1", "exec-1", "openai").await.unwrap() {
            ResolvedCredential::Opaque {
                value,
                allowed_hosts,
            } => {
                assert_eq!(value.as_str(), "sk-live");
                assert_eq!(allowed_hosts, ["api.openai.com"]);
            }
            ResolvedCredential::Fields { .. } => panic!("an opaque value came back as fields"),
        }
    }

    #[tokio::test]
    async fn a_structured_credential_resolves_field_by_field() {
        let (values, refs, _pool) = values_or_skip!("secret_values_fields");
        named(&refs, "tenant_db").await;

        values
            .put(
                "tenant_db",
                &fields(&[("host", "pg.internal"), ("port", "5432"), ("password", "p")]),
                &[],
            )
            .await
            .unwrap();
        values
            .grant("sbx-1", "exec-1", &["tenant_db".to_string()])
            .await
            .unwrap();

        match values
            .resolve("sbx-1", "exec-1", "tenant_db")
            .await
            .unwrap()
        {
            ResolvedCredential::Fields { fields, .. } => {
                assert_eq!(fields.get("host").map(|v| v.as_str()), Some("pg.internal"));
                assert_eq!(fields.get("port").map(|v| v.as_str()), Some("5432"));
                assert_eq!(fields.get("password").map(|v| v.as_str()), Some("p"));
                assert_eq!(fields.len(), 3);
            }
            ResolvedCredential::Opaque { .. } => panic!("fields came back as an opaque value"),
        }
    }

    #[tokio::test]
    async fn only_the_granted_triple_resolves() {
        let (values, refs, _pool) = values_or_skip!("secret_values_grant");
        for name in ["openai", "gh"] {
            named(&refs, name).await;
            values.put(name, &opaque("v"), &[]).await.unwrap();
        }
        named(&refs, "unwritten").await;

        assert!(
            denied(values.resolve("sbx-1", "exec-1", "openai").await),
            "no grant at all"
        );

        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();
        assert!(values.resolve("sbx-1", "exec-1", "openai").await.is_ok());
        assert!(
            denied(values.resolve("sbx-2", "exec-1", "openai").await),
            "the grant names another sandbox"
        );
        assert!(
            denied(values.resolve("sbx-1", "exec-2", "openai").await),
            "the grant names another execution"
        );
        assert!(
            denied(values.resolve("sbx-1", "exec-1", "gh").await),
            "the name is not in the grant"
        );

        // A name with a row and no version is refused the same way a name
        // nobody granted is: the broker turns both into one synthetic 403.
        values
            .grant("sbx-1", "exec-1", &["unwritten".to_string()])
            .await
            .unwrap();
        assert!(denied(values.resolve("sbx-1", "exec-1", "unwritten").await));
    }

    #[tokio::test]
    async fn a_regrant_replaces_the_names_and_a_revoke_removes_them() {
        let (values, refs, _pool) = values_or_skip!("secret_values_regrant");
        for name in ["openai", "gh"] {
            named(&refs, name).await;
            values.put(name, &opaque("v"), &[]).await.unwrap();
        }

        values
            .grant("sbx-1", "exec-1", &["openai".to_string(), "gh".to_string()])
            .await
            .unwrap();
        values
            .grant("sbx-1", "exec-1", &["gh".to_string()])
            .await
            .unwrap();
        assert!(
            denied(values.resolve("sbx-1", "exec-1", "openai").await),
            "a regrant that drops a name must not leave it readable"
        );
        assert!(values.resolve("sbx-1", "exec-1", "gh").await.is_ok());

        values.revoke("sbx-1", "exec-1").await.unwrap();
        assert!(denied(values.resolve("sbx-1", "exec-1", "gh").await));
    }

    #[tokio::test]
    async fn a_new_version_is_what_resolves_and_it_brings_its_own_pin() {
        let (values, refs, _pool) = values_or_skip!("secret_values_versions");
        named(&refs, "openai").await;
        values
            .put("openai", &opaque("v1"), &["api.openai.com".to_string()])
            .await
            .unwrap();
        assert_eq!(values.put("openai", &opaque("v2"), &[]).await.unwrap(), 2);
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        match values.resolve("sbx-1", "exec-1", "openai").await.unwrap() {
            ResolvedCredential::Opaque {
                value,
                allowed_hosts,
            } => {
                assert_eq!(value.as_str(), "v2");
                assert!(
                    allowed_hosts.is_empty(),
                    "the pin travels with the version, so an unpinned one is unpinned"
                );
            }
            ResolvedCredential::Fields { .. } => panic!("wrong kind"),
        }
    }

    #[tokio::test]
    async fn a_ciphertext_moved_under_another_name_does_not_open() {
        let (values, refs, pool) = values_or_skip!("secret_values_aad");
        for name in ["theirs", "mine"] {
            named(&refs, name).await;
        }
        values
            .put("theirs", &opaque("their-key"), &[])
            .await
            .unwrap();
        values.put("mine", &opaque("my-key"), &[]).await.unwrap();

        // What write access to the table buys: the row can be substituted,
        // and the value in it still does not come back out.
        sqlx::query(
            "UPDATE secret_values SET ciphertext = t.ciphertext, nonce = t.nonce \
             FROM secret_values t WHERE t.name = 'theirs' AND secret_values.name = 'mine'",
        )
        .execute(&pool)
        .await
        .unwrap();
        values
            .grant("sbx-1", "exec-1", &["mine".to_string()])
            .await
            .unwrap();

        assert!(matches!(
            values.resolve("sbx-1", "exec-1", "mine").await,
            Err(ResolveError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_delete_removes_every_version_and_missing_names_answers_for_values() {
        let (values, refs, _pool) = values_or_skip!("secret_values_delete");
        for name in ["openai", "unwritten"] {
            named(&refs, name).await;
        }
        values.put("openai", &opaque("v1"), &[]).await.unwrap();
        values.put("openai", &opaque("v2"), &[]).await.unwrap();

        let names = vec![
            "openai".to_string(),
            "unwritten".to_string(),
            "absent".to_string(),
        ];
        let missing = values.missing_names(&names).await.unwrap().unwrap();
        assert_eq!(missing, vec!["unwritten".to_string(), "absent".to_string()]);

        values.delete("openai").await.unwrap();
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();
        assert!(denied(values.resolve("sbx-1", "exec-1", "openai").await));
        assert_eq!(
            values.put("openai", &opaque("v3"), &[]).await.unwrap(),
            1,
            "versions restart once nothing is left to follow"
        );
    }

    #[tokio::test]
    async fn a_value_for_a_name_with_no_row_is_not_found() {
        let (values, _refs, _pool) = values_or_skip!("secret_values_no_row");
        assert!(matches!(
            values.put("nobody", &opaque("v"), &[]).await,
            Err(SecretsError::NotFound)
        ));
    }
}
