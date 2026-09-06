//! The two PostgreSQL tables behind `/secrets`: `secret_refs` here, and
//! `secret_values`/`secret_grants` in [`values`]; [`resolve_route`] is the
//! endpoint the broker reads the latter through.

pub mod resolve_route;
pub mod values;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aenv_core::secrets::{SecretMetadata, SecretRef, SecretRefStore, SecretsError};
use async_trait::async_trait;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

/// The `secret_refs` table: one row per secret, no value column.
pub struct PgSecretRefStore {
    pool: PgPool,
}

impl PgSecretRefStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const COLUMNS: &str = "secret_id, name, current_version, metadata, created_at_ms, updated_at_ms";

fn row_to_ref(row: PgRow) -> Result<SecretRef, SecretsError> {
    let metadata: serde_json::Value = row.try_get("metadata").map_err(unavailable)?;
    let metadata: BTreeMap<String, String> =
        serde_json::from_value(metadata).map_err(|err| unavailable(anyhow::Error::new(err)))?;
    Ok(SecretRef {
        secret_id: row.try_get("secret_id").map_err(unavailable)?,
        name: row.try_get("name").map_err(unavailable)?,
        current_version: row.try_get("current_version").map_err(unavailable)?,
        metadata,
        created_at: at_ms(row.try_get("created_at_ms").map_err(unavailable)?),
        updated_at: at_ms(row.try_get("updated_at_ms").map_err(unavailable)?),
    })
}

fn at_ms(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

pub(super) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(super) fn unavailable(err: impl Into<anyhow::Error>) -> SecretsError {
    SecretsError::Unavailable(err.into())
}

fn metadata_json(metadata: &SecretMetadata) -> serde_json::Value {
    serde_json::to_value(metadata).unwrap_or_else(|_| serde_json::json!({}))
}

#[async_trait]
impl SecretRefStore for PgSecretRefStore {
    async fn create(
        &self,
        secret_id: &str,
        name: &str,
        metadata: &SecretMetadata,
    ) -> Result<SecretRef, SecretsError> {
        let now = now_ms();
        let insert = sqlx::query(&format!(
            "INSERT INTO secret_refs ({COLUMNS}) VALUES ($1, $2, 0, $3, $4, $4) RETURNING {COLUMNS}"
        ))
        .bind(secret_id)
        .bind(name)
        .bind(metadata_json(metadata))
        .bind(now)
        .fetch_one(&self.pool)
        .await;
        match insert {
            Ok(row) => row_to_ref(row),
            Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                Err(SecretsError::AlreadyExists(name.to_string()))
            }
            Err(err) => Err(unavailable(err)),
        }
    }

    async fn set_current_version(
        &self,
        secret_id: &str,
        version: i64,
        metadata: Option<&SecretMetadata>,
    ) -> Result<SecretRef, SecretsError> {
        // Concurrent backend writes can reach this row out of order; the
        // column names the newest version the backend holds, so it never
        // walks back. Metadata is last-writer-wins.
        let row = sqlx::query(&format!(
            "UPDATE secret_refs SET current_version = GREATEST(current_version, $2), \
                    metadata = COALESCE($3, metadata), updated_at_ms = $4 \
              WHERE secret_id = $1 RETURNING {COLUMNS}"
        ))
        .bind(secret_id)
        .bind(version)
        .bind(metadata.map(metadata_json))
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(row_to_ref).ok_or(SecretsError::NotFound)?
    }

    async fn delete(&self, secret_id: &str) -> Result<Option<SecretRef>, SecretsError> {
        let row = sqlx::query(&format!(
            "DELETE FROM secret_refs WHERE secret_id = $1 RETURNING {COLUMNS}"
        ))
        .bind(secret_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(row_to_ref).transpose()
    }

    async fn get(&self, id_or_name: &str) -> Result<Option<SecretRef>, SecretsError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM secret_refs WHERE secret_id = $1 OR name = $1 \
              ORDER BY (secret_id = $1) DESC LIMIT 1"
        ))
        .bind(id_or_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(row_to_ref).transpose()
    }

    async fn list(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<SecretRef>, Option<String>), SecretsError> {
        let limit = limit.max(1);
        let fetch = i64::try_from(limit + 1).unwrap_or(i64::MAX);
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM secret_refs \
              WHERE $1::text IS NULL OR secret_id > $1 \
              ORDER BY secret_id LIMIT $2"
        ))
        .bind(after)
        .bind(fetch)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let mut page = rows
            .into_iter()
            .map(row_to_ref)
            .collect::<Result<Vec<_>, _>>()?;
        let next = if page.len() > limit {
            page.truncate(limit);
            page.last().map(|row| row.secret_id.clone())
        } else {
            None
        };
        Ok((page, next))
    }

    async fn missing_names(&self, names: &[String]) -> Result<Vec<String>, SecretsError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let present: Vec<String> =
            sqlx::query_scalar("SELECT name FROM secret_refs WHERE name = ANY($1)")
                .bind(names)
                .fetch_all(&self.pool)
                .await
                .map_err(unavailable)?;
        Ok(names
            .iter()
            .filter(|name| !present.contains(name))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;

    macro_rules! store_or_skip {
        ($name:literal) => {{
            let pool = isolated_schema_pool_or_skip!($name);
            migrate(&pool).await.expect("migration should succeed");
            PgSecretRefStore::new(pool)
        }};
    }

    #[tokio::test]
    async fn create_get_version_and_delete_round_trip() {
        let store = store_or_skip!("secret_refs_round_trip");
        let mut metadata = SecretMetadata::new();
        metadata.insert("owner".into(), "team-a".into());
        let created = store.create("sec_1", "openai", &metadata).await.unwrap();
        assert_eq!(created.current_version, 0);
        assert_eq!(created.metadata, metadata);

        let bumped = store.set_current_version("sec_1", 3, None).await.unwrap();
        assert_eq!(bumped.current_version, 3);
        assert_eq!(bumped.metadata, metadata);
        assert!(bumped.updated_at >= created.updated_at);

        let by_name = store.get("openai").await.unwrap().unwrap();
        let by_id = store.get("sec_1").await.unwrap().unwrap();
        assert_eq!(by_name, by_id);
        assert!(store.get("missing").await.unwrap().is_none());

        let removed = store.delete("sec_1").await.unwrap().unwrap();
        assert_eq!(removed.name, "openai");
        assert!(store.delete("sec_1").await.unwrap().is_none());
        assert!(matches!(
            store.set_current_version("sec_1", 4, None).await,
            Err(SecretsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn a_late_write_cannot_walk_the_current_version_back() {
        let store = store_or_skip!("secret_refs_version_monotonic");
        store
            .create("sec_1", "openai", &SecretMetadata::new())
            .await
            .unwrap();
        store.set_current_version("sec_1", 7, None).await.unwrap();

        let mut metadata = SecretMetadata::new();
        metadata.insert("owner".into(), "team-a".into());
        let late = store
            .set_current_version("sec_1", 3, Some(&metadata))
            .await
            .unwrap();
        assert_eq!(late.current_version, 7);
        assert_eq!(late.metadata, metadata);
        assert_eq!(
            store.get("sec_1").await.unwrap().unwrap().current_version,
            7
        );
    }

    #[tokio::test]
    async fn a_duplicate_name_is_a_conflict_not_an_outage() {
        let store = store_or_skip!("secret_refs_duplicate");
        store
            .create("sec_a", "gh", &SecretMetadata::new())
            .await
            .unwrap();
        assert!(matches!(
            store.create("sec_b", "gh", &SecretMetadata::new()).await,
            Err(SecretsError::AlreadyExists(name)) if name == "gh"
        ));
    }

    #[tokio::test]
    async fn listing_pages_by_id_and_missing_names_are_reported() {
        let store = store_or_skip!("secret_refs_list");
        for (id, name) in [("sec_1", "a"), ("sec_2", "b"), ("sec_3", "c")] {
            store
                .create(id, name, &SecretMetadata::new())
                .await
                .unwrap();
        }
        let (first, next) = store.list(None, 2).await.unwrap();
        assert_eq!(
            first.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        let next = next.expect("a third row remains");
        let (rest, done) = store.list(Some(&next), 2).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert!(done.is_none());

        let missing = store
            .missing_names(&["a".into(), "zzz".into(), "c".into()])
            .await
            .unwrap();
        assert_eq!(missing, vec!["zzz"]);
        assert!(store.missing_names(&[]).await.unwrap().is_empty());
    }
}
