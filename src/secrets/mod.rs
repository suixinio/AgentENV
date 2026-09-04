//! The api half's view of secrets: names, versions and grants. Values pass
//! through [`SecretsBackend`] and are never stored, logged or returned here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::orchestrator::grants::GrantIssuer;
use crate::sandbox::network::policy::{is_valid_secret_name, MAX_SECRET_NAME_LEN};
use crate::types::{ExecutionId, SandboxId};

pub const SECRET_ID_PREFIX: &str = "sec_";
pub const MAX_METADATA_ENTRIES: usize = 32;
pub const MAX_METADATA_KEY_BYTES: usize = 128;
pub const MAX_METADATA_VALUE_BYTES: usize = 1024;
pub const MAX_METADATA_TOTAL_BYTES: usize = 8192;

/// A secret value in transit. Wiped on drop; `Debug` never shows it.
#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString([redacted])")
    }
}

pub type SecretMetadata = BTreeMap<String, String>;

/// What the API returns about a secret. It carries no value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretRef {
    pub secret_id: String,
    pub name: String,
    pub current_version: i64,
    pub metadata: SecretMetadata,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    #[error("secret name must match [a-zA-Z0-9_-]{{1,{MAX_SECRET_NAME_LEN}}}")]
    InvalidName,
    #[error("{0}")]
    InvalidMetadata(String),
    #[error("secret value must not be empty")]
    EmptyValue,
    #[error("secret not found")]
    NotFound,
    #[error("a secret named {0:?} already exists")]
    AlreadyExists(String),
    #[error("secrets store unavailable: {0}")]
    Unavailable(#[source] anyhow::Error),
}

/// The metadata rows: id, name, version and customer metadata, no values.
#[async_trait]
pub trait SecretRefStore: Send + Sync {
    /// Inserts a row at version 0; `AlreadyExists` when the name is taken.
    async fn create(
        &self,
        secret_id: &str,
        name: &str,
        metadata: &SecretMetadata,
    ) -> Result<SecretRef, SecretsError>;
    /// Records the version the backend just wrote and, when given, new metadata.
    async fn set_current_version(
        &self,
        secret_id: &str,
        version: i64,
        metadata: Option<&SecretMetadata>,
    ) -> Result<SecretRef, SecretsError>;
    async fn delete(&self, secret_id: &str) -> Result<Option<SecretRef>, SecretsError>;
    /// Looks a secret up by id or by name.
    async fn get(&self, id_or_name: &str) -> Result<Option<SecretRef>, SecretsError>;
    /// Rows ordered by id, starting after `after`, at most `limit`. Returns
    /// the cursor for the next page when more rows exist.
    async fn list(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<SecretRef>, Option<String>), SecretsError>;
    /// The subset of `names` with no row.
    async fn missing_names(&self, names: &[String]) -> Result<Vec<String>, SecretsError>;
}

/// The value store. Writes return the version they created; grants make a
/// (sandbox, execution) able to read the named values.
#[async_trait]
pub trait SecretsBackend: Send + Sync {
    async fn put(&self, name: &str, value: &SecretString) -> Result<i64, SecretsError>;
    async fn delete(&self, name: &str) -> Result<(), SecretsError>;
    async fn grant(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        names: &[String],
    ) -> Result<(), SecretsError>;
    async fn revoke(&self, sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError>;

    /// Which of `names` this backend does not know, for a backend that owns
    /// the names as well as the values. `None` leaves the ref store as the
    /// authority, which is what a value store answers.
    async fn missing_names(&self, _names: &[String]) -> Result<Option<Vec<String>>, SecretsError> {
        Ok(None)
    }
}

/// The `/secrets` operations and grant issuance, over a ref store and a
/// value backend.
pub struct SecretsService {
    refs: Arc<dyn SecretRefStore>,
    values: Arc<dyn SecretsBackend>,
}

impl SecretsService {
    pub fn new(refs: Arc<dyn SecretRefStore>, values: Arc<dyn SecretsBackend>) -> Self {
        Self { refs, values }
    }

    pub async fn create(
        &self,
        name: &str,
        value: SecretString,
        metadata: SecretMetadata,
    ) -> Result<SecretRef, SecretsError> {
        validate_name(name)?;
        validate_metadata(&metadata)?;
        if value.is_empty() {
            return Err(SecretsError::EmptyValue);
        }
        let secret_id = new_secret_id();
        self.refs.create(&secret_id, name, &metadata).await?;
        let version = match self.values.put(name, &value).await {
            Ok(version) => version,
            Err(err) => {
                if let Err(rollback) = self.refs.delete(&secret_id).await {
                    tracing::warn!(
                        secret_id,
                        error = %rollback,
                        "failed to remove the row of a secret whose value was never stored"
                    );
                }
                return Err(err);
            }
        };
        drop(value);
        match self
            .refs
            .set_current_version(&secret_id, version, None)
            .await
        {
            Ok(secret) => Ok(secret),
            Err(SecretsError::NotFound) => {
                self.discard_orphaned_value(name).await;
                Err(SecretsError::NotFound)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn update(
        &self,
        id_or_name: &str,
        value: SecretString,
        metadata: Option<SecretMetadata>,
    ) -> Result<SecretRef, SecretsError> {
        if let Some(metadata) = &metadata {
            validate_metadata(metadata)?;
        }
        if value.is_empty() {
            return Err(SecretsError::EmptyValue);
        }
        let existing = self
            .refs
            .get(id_or_name)
            .await?
            .ok_or(SecretsError::NotFound)?;
        let version = self.values.put(&existing.name, &value).await?;
        drop(value);
        match self
            .refs
            .set_current_version(&existing.secret_id, version, metadata.as_ref())
            .await
        {
            Err(SecretsError::NotFound) => {
                self.discard_orphaned_value(&existing.name).await;
                Err(SecretsError::NotFound)
            }
            other => other,
        }
    }

    // Drops a value whose row is gone, so no value outlives the row that
    // makes it reachable. Addressed by name, which is all the backend
    // exposes, so a concurrent recreate of the same name is not fenced off.
    async fn discard_orphaned_value(&self, name: &str) {
        if let Err(err) = self.values.delete(name).await {
            tracing::warn!(
                secret = name,
                error = %err,
                "failed to delete the value of a secret whose row disappeared mid-write"
            );
        }
    }

    pub async fn delete(&self, id_or_name: &str) -> Result<(), SecretsError> {
        let existing = self
            .refs
            .get(id_or_name)
            .await?
            .ok_or(SecretsError::NotFound)?;
        self.values.delete(&existing.name).await?;
        self.refs.delete(&existing.secret_id).await?;
        Ok(())
    }

    pub async fn get(&self, id_or_name: &str) -> Result<SecretRef, SecretsError> {
        self.refs
            .get(id_or_name)
            .await?
            .ok_or(SecretsError::NotFound)
    }

    pub async fn list(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<SecretRef>, Option<String>), SecretsError> {
        self.refs.list(after, limit.max(1)).await
    }

    /// `Ok(())` when every name has a row; otherwise the missing names.
    pub async fn ensure_names_exist(&self, names: &BTreeSet<String>) -> Result<(), MissingSecrets> {
        if names.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = names.iter().cloned().collect();
        let missing = match self
            .values
            .missing_names(&names)
            .await
            .map_err(MissingSecrets::Store)?
        {
            Some(missing) => missing,
            None => self
                .refs
                .missing_names(&names)
                .await
                .map_err(MissingSecrets::Store)?,
        };
        if missing.is_empty() {
            Ok(())
        } else {
            Err(MissingSecrets::Missing(missing))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MissingSecrets {
    #[error("unknown secret name(s): {}", .0.join(", "))]
    Missing(Vec<String>),
    #[error(transparent)]
    Store(SecretsError),
}

#[async_trait]
impl GrantIssuer for SecretsService {
    async fn grant(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        names: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        let names: Vec<String> = names.iter().cloned().collect();
        self.values
            .grant(&sandbox_id.to_string(), &execution_id.to_string(), &names)
            .await?;
        Ok(())
    }

    async fn revoke(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()> {
        self.values
            .revoke(&sandbox_id.to_string(), &execution_id.to_string())
            .await?;
        Ok(())
    }
}

pub fn validate_name(name: &str) -> Result<(), SecretsError> {
    if is_valid_secret_name(name) && !name.starts_with(SECRET_ID_PREFIX) {
        Ok(())
    } else {
        Err(SecretsError::InvalidName)
    }
}

pub fn validate_metadata(metadata: &SecretMetadata) -> Result<(), SecretsError> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(SecretsError::InvalidMetadata(format!(
            "metadata has {} entries; at most {MAX_METADATA_ENTRIES} are allowed",
            metadata.len()
        )));
    }
    let mut total = 0;
    for (key, value) in metadata {
        if key.is_empty() || key.len() > MAX_METADATA_KEY_BYTES {
            return Err(SecretsError::InvalidMetadata(format!(
                "metadata key {key:?} must be 1 to {MAX_METADATA_KEY_BYTES} bytes"
            )));
        }
        if value.len() > MAX_METADATA_VALUE_BYTES {
            return Err(SecretsError::InvalidMetadata(format!(
                "metadata value for {key:?} exceeds {MAX_METADATA_VALUE_BYTES} bytes"
            )));
        }
        total += key.len() + value.len();
    }
    if total > MAX_METADATA_TOTAL_BYTES {
        return Err(SecretsError::InvalidMetadata(format!(
            "metadata totals {total} bytes; at most {MAX_METADATA_TOTAL_BYTES} are allowed"
        )));
    }
    Ok(())
}

pub fn new_secret_id() -> String {
    format!("{SECRET_ID_PREFIX}{}", uuid::Uuid::now_v7().simple())
}

/// In-memory implementations of both traits for tests and for deployments
/// that run without a store.
pub mod memory {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Mutex;
    use std::time::SystemTime;

    use async_trait::async_trait;

    use super::{
        SecretMetadata, SecretRef, SecretRefStore, SecretString, SecretsBackend, SecretsError,
    };

    #[derive(Default)]
    pub struct InMemorySecretRefStore {
        rows: Mutex<BTreeMap<String, SecretRef>>,
    }

    #[async_trait]
    impl SecretRefStore for InMemorySecretRefStore {
        async fn create(
            &self,
            secret_id: &str,
            name: &str,
            metadata: &SecretMetadata,
        ) -> Result<SecretRef, SecretsError> {
            let mut rows = self.rows.lock().unwrap();
            if rows.values().any(|row| row.name == name) {
                return Err(SecretsError::AlreadyExists(name.to_string()));
            }
            let now = SystemTime::now();
            let row = SecretRef {
                secret_id: secret_id.to_string(),
                name: name.to_string(),
                current_version: 0,
                metadata: metadata.clone(),
                created_at: now,
                updated_at: now,
            };
            rows.insert(secret_id.to_string(), row.clone());
            Ok(row)
        }

        async fn set_current_version(
            &self,
            secret_id: &str,
            version: i64,
            metadata: Option<&SecretMetadata>,
        ) -> Result<SecretRef, SecretsError> {
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(secret_id).ok_or(SecretsError::NotFound)?;
            row.current_version = version;
            if let Some(metadata) = metadata {
                row.metadata = metadata.clone();
            }
            row.updated_at = SystemTime::now();
            Ok(row.clone())
        }

        async fn delete(&self, secret_id: &str) -> Result<Option<SecretRef>, SecretsError> {
            Ok(self.rows.lock().unwrap().remove(secret_id))
        }

        async fn get(&self, id_or_name: &str) -> Result<Option<SecretRef>, SecretsError> {
            let rows = self.rows.lock().unwrap();
            Ok(rows
                .get(id_or_name)
                .or_else(|| rows.values().find(|row| row.name == id_or_name))
                .cloned())
        }

        async fn list(
            &self,
            after: Option<&str>,
            limit: usize,
        ) -> Result<(Vec<SecretRef>, Option<String>), SecretsError> {
            let rows = self.rows.lock().unwrap();
            let page: Vec<SecretRef> = rows
                .values()
                .filter(|row| after.is_none_or(|after| row.secret_id.as_str() > after))
                .take(limit + 1)
                .cloned()
                .collect();
            if page.len() > limit {
                let mut page = page;
                page.truncate(limit);
                let next = page.last().map(|row| row.secret_id.clone());
                Ok((page, next))
            } else {
                Ok((page, None))
            }
        }

        async fn missing_names(&self, names: &[String]) -> Result<Vec<String>, SecretsError> {
            let rows = self.rows.lock().unwrap();
            let present: HashSet<&str> = rows.values().map(|row| row.name.as_str()).collect();
            Ok(names
                .iter()
                .filter(|name| !present.contains(name.as_str()))
                .cloned()
                .collect())
        }
    }

    /// Holds values and grants in memory. Exposes what a test needs to
    /// assert on; never used outside tests and embedded smoke runs.
    #[derive(Default)]
    pub struct InMemorySecretsBackend {
        values: Mutex<HashMap<String, (i64, SecretString)>>,
        grants: Mutex<HashMap<(String, String), Vec<String>>>,
        pub fail_puts: std::sync::atomic::AtomicBool,
    }

    impl InMemorySecretsBackend {
        pub fn version_of(&self, name: &str) -> Option<i64> {
            self.values.lock().unwrap().get(name).map(|(v, _)| *v)
        }

        pub fn grants_for(&self, sandbox_id: &str, execution_id: &str) -> Option<Vec<String>> {
            self.grants
                .lock()
                .unwrap()
                .get(&(sandbox_id.to_string(), execution_id.to_string()))
                .cloned()
        }

        pub fn value_of(&self, name: &str) -> Option<SecretString> {
            self.values
                .lock()
                .unwrap()
                .get(name)
                .map(|(_, value)| value.clone())
        }
    }

    #[async_trait]
    impl SecretsBackend for InMemorySecretsBackend {
        async fn put(&self, name: &str, value: &SecretString) -> Result<i64, SecretsError> {
            if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SecretsError::Unavailable(anyhow::anyhow!(
                    "store refused the write"
                )));
            }
            let mut values = self.values.lock().unwrap();
            let version = values.get(name).map(|(v, _)| *v).unwrap_or(0) + 1;
            values.insert(name.to_string(), (version, value.clone()));
            Ok(version)
        }

        async fn delete(&self, name: &str) -> Result<(), SecretsError> {
            self.values.lock().unwrap().remove(name);
            Ok(())
        }

        async fn grant(
            &self,
            sandbox_id: &str,
            execution_id: &str,
            names: &[String],
        ) -> Result<(), SecretsError> {
            self.grants.lock().unwrap().insert(
                (sandbox_id.to_string(), execution_id.to_string()),
                names.to_vec(),
            );
            Ok(())
        }

        async fn revoke(&self, sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError> {
            self.grants
                .lock()
                .unwrap()
                .remove(&(sandbox_id.to_string(), execution_id.to_string()));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::memory::{InMemorySecretRefStore, InMemorySecretsBackend};
    use super::*;

    fn service() -> (SecretsService, Arc<InMemorySecretsBackend>) {
        let backend = Arc::new(InMemorySecretsBackend::default());
        let service =
            SecretsService::new(Arc::new(InMemorySecretRefStore::default()), backend.clone());
        (service, backend)
    }

    fn value(s: &str) -> SecretString {
        SecretString::new(s.to_string())
    }

    // Stands in for a row deleted by a concurrent request between the value
    // write and the version update.
    #[derive(Default)]
    struct VanishingRowStore {
        inner: InMemorySecretRefStore,
        vanish: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl SecretRefStore for VanishingRowStore {
        async fn create(
            &self,
            secret_id: &str,
            name: &str,
            metadata: &SecretMetadata,
        ) -> Result<SecretRef, SecretsError> {
            self.inner.create(secret_id, name, metadata).await
        }

        async fn set_current_version(
            &self,
            secret_id: &str,
            version: i64,
            metadata: Option<&SecretMetadata>,
        ) -> Result<SecretRef, SecretsError> {
            if self.vanish.load(Ordering::SeqCst) {
                self.inner.delete(secret_id).await?;
                return Err(SecretsError::NotFound);
            }
            self.inner
                .set_current_version(secret_id, version, metadata)
                .await
        }

        async fn delete(&self, secret_id: &str) -> Result<Option<SecretRef>, SecretsError> {
            self.inner.delete(secret_id).await
        }

        async fn get(&self, id_or_name: &str) -> Result<Option<SecretRef>, SecretsError> {
            self.inner.get(id_or_name).await
        }

        async fn list(
            &self,
            after: Option<&str>,
            limit: usize,
        ) -> Result<(Vec<SecretRef>, Option<String>), SecretsError> {
            self.inner.list(after, limit).await
        }

        async fn missing_names(&self, names: &[String]) -> Result<Vec<String>, SecretsError> {
            self.inner.missing_names(names).await
        }
    }

    fn vanishing_service() -> (
        SecretsService,
        Arc<VanishingRowStore>,
        Arc<InMemorySecretsBackend>,
    ) {
        let refs = Arc::new(VanishingRowStore::default());
        let backend = Arc::new(InMemorySecretsBackend::default());
        let service = SecretsService::new(refs.clone(), backend.clone());
        (service, refs, backend)
    }

    #[tokio::test]
    async fn a_create_whose_row_vanished_reports_not_found_and_leaves_no_value() {
        let (service, refs, backend) = vanishing_service();
        refs.vanish.store(true, Ordering::SeqCst);

        assert!(matches!(
            service
                .create("openai", value("sk"), SecretMetadata::new())
                .await,
            Err(SecretsError::NotFound)
        ));
        assert!(
            backend.value_of("openai").is_none(),
            "a value with no row is unreachable and must not be left behind"
        );
    }

    #[tokio::test]
    async fn an_update_whose_row_vanished_reports_not_found_and_leaves_no_value() {
        let (service, refs, backend) = vanishing_service();
        service
            .create("gh", value("v1"), SecretMetadata::new())
            .await
            .unwrap();
        refs.vanish.store(true, Ordering::SeqCst);

        assert!(matches!(
            service.update("gh", value("v2"), None).await,
            Err(SecretsError::NotFound)
        ));
        assert!(
            backend.value_of("gh").is_none(),
            "the version this call wrote must not outlive the row it belonged to"
        );
    }

    #[test]
    fn secret_string_debug_is_redacted() {
        let secret = value("sk-live-123");
        assert_eq!(format!("{secret:?}"), "SecretString([redacted])");
        assert_eq!(secret.expose(), "sk-live-123");
    }

    #[tokio::test]
    async fn create_stores_the_value_in_the_backend_and_the_row_without_it() {
        let (service, backend) = service();
        let created = service
            .create("openai", value("sk"), SecretMetadata::new())
            .await
            .unwrap();
        assert!(created.secret_id.starts_with("sec_"));
        assert_eq!(created.name, "openai");
        assert_eq!(created.current_version, 1);
        assert_eq!(backend.version_of("openai"), Some(1));
        assert!(!format!("{created:?}").contains("sk"));

        let by_name = service.get("openai").await.unwrap();
        let by_id = service.get(&created.secret_id).await.unwrap();
        assert_eq!(by_name, by_id);
    }

    #[tokio::test]
    async fn a_failed_backend_write_leaves_no_row_behind() {
        let (service, backend) = service();
        backend.fail_puts.store(true, Ordering::SeqCst);
        let err = service
            .create("openai", value("sk"), SecretMetadata::new())
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SecretsError::Unavailable(_)));
        assert!(matches!(
            service.get("openai").await,
            Err(SecretsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn names_are_validated_and_unique() {
        let (service, _) = service();
        for bad in ["", "with/slash", "sec_reserved", &"a".repeat(129)] {
            assert!(matches!(
                service.create(bad, value("v"), SecretMetadata::new()).await,
                Err(SecretsError::InvalidName)
            ));
        }
        assert!(matches!(
            service
                .create("empty", value(""), SecretMetadata::new())
                .await,
            Err(SecretsError::EmptyValue)
        ));
        service
            .create("gh", value("v"), SecretMetadata::new())
            .await
            .unwrap();
        assert!(matches!(
            service.create("gh", value("v2"), SecretMetadata::new()).await,
            Err(SecretsError::AlreadyExists(name)) if name == "gh"
        ));
    }

    #[tokio::test]
    async fn update_appends_a_version_and_delete_removes_value_and_row() {
        let (service, backend) = service();
        let created = service
            .create("gh", value("v1"), SecretMetadata::new())
            .await
            .unwrap();
        let mut metadata = SecretMetadata::new();
        metadata.insert("owner".into(), "team-a".into());
        let updated = service
            .update(&created.secret_id, value("v2"), Some(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(updated.current_version, 2);
        assert_eq!(updated.metadata, metadata);
        assert_eq!(backend.value_of("gh").unwrap().expose(), "v2");

        service.delete("gh").await.unwrap();
        assert!(backend.value_of("gh").is_none());
        assert!(matches!(
            service.get("gh").await,
            Err(SecretsError::NotFound)
        ));
        assert!(matches!(
            service.delete("gh").await,
            Err(SecretsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn listing_pages_by_id_with_a_cursor() {
        let (service, _) = service();
        for name in ["a", "b", "c"] {
            service
                .create(name, value("v"), SecretMetadata::new())
                .await
                .unwrap();
        }
        let (first, next) = service.list(None, 2).await.unwrap();
        assert_eq!(first.len(), 2);
        let next = next.expect("a third row remains");
        let (rest, done) = service.list(Some(&next), 2).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert!(done.is_none());
        assert!(first
            .iter()
            .chain(rest.iter())
            .map(|s| s.name.as_str())
            .eq(["a", "b", "c"]));
    }

    /// A backend that owns its names, the way an external resolver does.
    #[derive(Default)]
    struct NameOwningBackend {
        known: Vec<String>,
    }

    #[async_trait]
    impl SecretsBackend for NameOwningBackend {
        async fn put(&self, _: &str, _: &SecretString) -> Result<i64, SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!(
                "owned elsewhere"
            )))
        }
        async fn delete(&self, _: &str) -> Result<(), SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!(
                "owned elsewhere"
            )))
        }
        async fn grant(&self, _: &str, _: &str, _: &[String]) -> Result<(), SecretsError> {
            Ok(())
        }
        async fn revoke(&self, _: &str, _: &str) -> Result<(), SecretsError> {
            Ok(())
        }
        async fn missing_names(
            &self,
            names: &[String],
        ) -> Result<Option<Vec<String>>, SecretsError> {
            Ok(Some(
                names
                    .iter()
                    .filter(|name| !self.known.contains(name))
                    .cloned()
                    .collect(),
            ))
        }
    }

    #[tokio::test]
    async fn a_backend_that_owns_its_names_answers_instead_of_the_ref_store() {
        let service = SecretsService::new(
            Arc::new(InMemorySecretRefStore::default()),
            Arc::new(NameOwningBackend {
                known: vec!["tenant_db".to_string()],
            }),
        );
        let names = |names: &[&str]| -> BTreeSet<String> {
            names.iter().map(|name| name.to_string()).collect()
        };

        // The ref store holds no row for either name, so an answer that only
        // consulted it would call both of them missing.
        service
            .ensure_names_exist(&names(&["tenant_db"]))
            .await
            .unwrap();
        let err = service
            .ensure_names_exist(&names(&["tenant_db", "absent"]))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, MissingSecrets::Missing(missing) if missing == ["absent"]));
    }

    #[tokio::test]
    async fn ensure_names_exist_reports_only_the_missing_ones() {
        let (service, _) = service();
        service
            .create("present", value("v"), SecretMetadata::new())
            .await
            .unwrap();
        let names: BTreeSet<String> = ["present", "absent"]
            .into_iter()
            .map(String::from)
            .collect();
        match service.ensure_names_exist(&names).await {
            Err(MissingSecrets::Missing(missing)) => assert_eq!(missing, vec!["absent"]),
            other => panic!("expected the missing name, got {other:?}"),
        }
        let only_present: BTreeSet<String> = ["present".to_string()].into_iter().collect();
        assert!(service.ensure_names_exist(&only_present).await.is_ok());
        assert!(service.ensure_names_exist(&BTreeSet::new()).await.is_ok());
    }

    #[tokio::test]
    async fn grants_are_keyed_by_sandbox_and_execution() {
        let (service, backend) = service();
        let sandbox = SandboxId::new();
        let execution = ExecutionId::new();
        let names: BTreeSet<String> = ["openai".to_string()].into_iter().collect();
        service.grant(sandbox, execution, &names).await.unwrap();
        assert_eq!(
            backend.grants_for(&sandbox.to_string(), &execution.to_string()),
            Some(vec!["openai".to_string()])
        );
        service.revoke(sandbox, execution).await.unwrap();
        assert!(backend
            .grants_for(&sandbox.to_string(), &execution.to_string())
            .is_none());
    }

    #[test]
    fn metadata_limits_follow_the_public_contract() {
        let mut ok = SecretMetadata::new();
        ok.insert("k".into(), "v".into());
        assert!(validate_metadata(&ok).is_ok());

        let mut too_many = SecretMetadata::new();
        for i in 0..=MAX_METADATA_ENTRIES {
            too_many.insert(format!("k{i}"), "v".into());
        }
        assert!(validate_metadata(&too_many).is_err());

        let mut long_key = SecretMetadata::new();
        long_key.insert("k".repeat(MAX_METADATA_KEY_BYTES + 1), "v".into());
        assert!(validate_metadata(&long_key).is_err());

        let mut long_value = SecretMetadata::new();
        long_value.insert("k".into(), "v".repeat(MAX_METADATA_VALUE_BYTES + 1));
        assert!(validate_metadata(&long_value).is_err());

        let mut total = SecretMetadata::new();
        for i in 0..9 {
            total.insert(format!("key-{i:03}"), "v".repeat(MAX_METADATA_VALUE_BYTES));
        }
        assert!(validate_metadata(&total).is_err());
    }
}
