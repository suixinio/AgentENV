//! Names, versions and grants. Values pass through [`SecretsBackend`] and are
//! never stored, logged or returned here.
//!
//! Only the deciding half serves any of this: a node holds no secret store and
//! never learns a name, which is why [`SecretKind`] is the one piece that stays
//! in `aenv-core`, where the policy check and the grant issuer read it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::orchestrator::grants::GrantIssuer;
use crate::sandbox::network::policy::{is_valid_secret_name, MAX_SECRET_NAME_LEN};
use crate::types::{ExecutionId, SandboxId};

pub use aenv_core::secret_kind::SecretKind;

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

/// The set of named scalars a handler that authenticates to the upstream
/// itself is given. Keys are field names, not secrets.
pub type SecretFields = BTreeMap<String, SecretString>;

pub const MAX_SECRET_FIELDS: usize = 32;
pub const MAX_SECRET_FIELD_KEY_LEN: usize = 64;

/// What a `/secrets` write carries. `Opaque` is one value substituted into a
/// header; `Fields` is a credential a protocol handler takes apart itself,
/// which is the only form a `postgres` endpoint can use.
#[derive(Clone, Debug)]
pub enum SecretValue {
    Opaque(SecretString),
    Fields(SecretFields),
}

impl SecretValue {
    pub fn kind(&self) -> SecretKind {
        match self {
            Self::Opaque(_) => SecretKind::Opaque,
            Self::Fields(_) => SecretKind::Fields,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Opaque(value) => value.is_empty(),
            Self::Fields(fields) => fields.is_empty(),
        }
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
    #[error("secret name must not start with {SECRET_ID_PREFIX}, which is reserved for secret identifiers")]
    ReservedName,
    #[error("{0}")]
    InvalidMetadata(String),
    #[error("secret value must not be empty")]
    EmptyValue,
    #[error("fields must carry at least one entry")]
    EmptyFields,
    #[error("secret not found")]
    NotFound,
    #[error("a secret named {0:?} already exists")]
    AlreadyExists(String),
    #[error(
        "secret {name:?} holds {stored}; a new version keeps the shape a secret was created with, \
         so a {offered} credential is a new secret"
    )]
    ShapeChanged {
        name: String,
        stored: SecretKind,
        offered: SecretKind,
    },
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

/// One grant row: the incarnation it names. The reaper compares it with the
/// live records and revokes what no record backs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub sandbox_id: String,
    pub execution_id: String,
}

/// The value store. Writes return the version they created; grants make a
/// (sandbox, execution) able to read the named values.
#[async_trait]
pub trait SecretsBackend: Send + Sync {
    /// `allowed_hosts` pins the value to the hosts it may be sent to; empty
    /// leaves the rule that names it as the only bound. It is stored with the
    /// value because the broker reads it there, not from the ref row. A
    /// backend that shares its storage with the ref row records the new
    /// version there in the same transaction, so what it serves is never
    /// ahead of what `GET /secrets/{id}` reports.
    async fn put(
        &self,
        name: &str,
        value: &SecretValue,
        allowed_hosts: &[String],
    ) -> Result<i64, SecretsError>;
    async fn delete(&self, name: &str) -> Result<(), SecretsError>;
    async fn grant(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        names: &[String],
    ) -> Result<(), SecretsError>;
    async fn revoke(&self, sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError>;

    /// The shape each of `names` is stored in, for a backend that owns the
    /// names as well as the values; a name absent from the map has no
    /// value. `None` leaves the ref store as the authority on existence and
    /// leaves the shape unchecked, which is what a value store answers.
    async fn kinds_of(
        &self,
        _names: &[String],
    ) -> Result<Option<BTreeMap<String, SecretKind>>, SecretsError> {
        Ok(None)
    }

    /// Grants recorded before `granted_before`, oldest first and at most
    /// `limit`, for the reaper. A backend that keeps no grants answers none.
    async fn grants_before(
        &self,
        _granted_before: SystemTime,
        _limit: usize,
    ) -> Result<Vec<Grant>, SecretsError> {
        Ok(Vec::new())
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
        value: SecretValue,
        metadata: SecretMetadata,
        allowed_hosts: Vec<String>,
    ) -> Result<SecretRef, SecretsError> {
        validate_name(name)?;
        validate_metadata(&metadata)?;
        let allowed_hosts = validate_allowed_hosts(allowed_hosts)?;
        validate_value(&value)?;
        validate_pin_fits(&value, &allowed_hosts)?;
        let secret_id = new_secret_id();
        self.refs.create(&secret_id, name, &metadata).await?;
        let version = match self.values.put(name, &value, &allowed_hosts).await {
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

    /// A new version replaces the pin as well as the value: the pin lives
    /// with the value, so a version written without one is unpinned. The
    /// shape does not change: a policy that reads the name was checked
    /// against it, and the next version has to satisfy the same check.
    pub async fn update(
        &self,
        id_or_name: &str,
        value: SecretValue,
        metadata: Option<SecretMetadata>,
        allowed_hosts: Vec<String>,
    ) -> Result<SecretRef, SecretsError> {
        if let Some(metadata) = &metadata {
            validate_metadata(metadata)?;
        }
        let allowed_hosts = validate_allowed_hosts(allowed_hosts)?;
        validate_value(&value)?;
        validate_pin_fits(&value, &allowed_hosts)?;
        let existing = self
            .refs
            .get(id_or_name)
            .await?
            .ok_or(SecretsError::NotFound)?;
        let stored = self
            .values
            .kinds_of(std::slice::from_ref(&existing.name))
            .await?
            .and_then(|kinds| kinds.get(&existing.name).copied());
        if let Some(stored) = stored.filter(|stored| *stored != value.kind()) {
            return Err(SecretsError::ShapeChanged {
                name: existing.name,
                stored,
                offered: value.kind(),
            });
        }
        let version = self
            .values
            .put(&existing.name, &value, &allowed_hosts)
            .await?;
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

    /// `Ok(())` when every name has a value in the shape its use needs. A
    /// backend that cannot say which shape it holds is checked for
    /// existence alone, against the ref rows.
    pub async fn ensure_usable(
        &self,
        wanted: &BTreeMap<String, SecretKind>,
    ) -> Result<(), UnusableSecrets> {
        if wanted.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = wanted.keys().cloned().collect();
        let stored = self
            .values
            .kinds_of(&names)
            .await
            .map_err(UnusableSecrets::Store)?;
        let Some(stored) = stored else {
            let missing = self
                .refs
                .missing_names(&names)
                .await
                .map_err(UnusableSecrets::Store)?;
            return if missing.is_empty() {
                Ok(())
            } else {
                Err(UnusableSecrets::Missing(missing))
            };
        };
        let missing: Vec<String> = names
            .iter()
            .filter(|name| !stored.contains_key(name.as_str()))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(UnusableSecrets::Missing(missing));
        }
        let mismatched: Vec<ShapeMismatch> = wanted
            .iter()
            .filter_map(|(name, needed)| {
                let stored = *stored.get(name)?;
                (stored != *needed).then(|| ShapeMismatch {
                    name: name.clone(),
                    stored,
                    needed: *needed,
                })
            })
            .collect();
        if mismatched.is_empty() {
            Ok(())
        } else {
            Err(UnusableSecrets::WrongShape(mismatched))
        }
    }

    /// Grants older than `min_age` whose incarnation the caller may compare
    /// with its live records. Rows the caller cannot name are skipped.
    async fn grants_older_than(
        &self,
        min_age: Duration,
        limit: usize,
    ) -> Result<Vec<(SandboxId, ExecutionId)>, SecretsError> {
        let before = SystemTime::now()
            .checked_sub(min_age)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        Ok(self
            .values
            .grants_before(before, limit)
            .await?
            .into_iter()
            .filter_map(|grant| {
                Some((
                    SandboxId::parse_str(&grant.sandbox_id).ok()?,
                    ExecutionId::parse_str(&grant.execution_id).ok()?,
                ))
            })
            .collect())
    }
}

/// One name a policy uses in a shape other than the one stored under it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShapeMismatch {
    pub name: String,
    pub stored: SecretKind,
    pub needed: SecretKind,
}

impl fmt::Display for ShapeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} holds {} but is used as {}",
            self.name, self.stored, self.needed
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnusableSecrets {
    #[error("unknown secret name(s): {}", .0.join(", "))]
    Missing(Vec<String>),
    #[error(
        "secret(s) stored in another shape than the policy uses: {}",
        .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
    )]
    WrongShape(Vec<ShapeMismatch>),
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

    async fn unusable_names(&self, wanted: &BTreeMap<String, SecretKind>) -> Option<Vec<String>> {
        match self.ensure_usable(wanted).await {
            Ok(()) => Some(Vec::new()),
            Err(UnusableSecrets::Missing(missing)) => Some(missing),
            Err(UnusableSecrets::WrongShape(mismatched)) => Some(
                mismatched
                    .into_iter()
                    .map(|mismatch| mismatch.to_string())
                    .collect(),
            ),
            // The grant this answer precedes fails loudly on the same
            // outage, so reporting it here would only duplicate it.
            Err(UnusableSecrets::Store(err)) => {
                tracing::debug!(error = %format_args!("{err:#}"), "secrets store could not say which names exist");
                None
            }
        }
    }

    async fn stale_grant_candidates(
        &self,
        min_age: Duration,
        limit: usize,
    ) -> anyhow::Result<Vec<(SandboxId, ExecutionId)>> {
        Ok(self.grants_older_than(min_age, limit).await?)
    }
}

/// A `fields` credential is rejected on the shape of its keys, never on
/// its values: an empty password is a real configuration, an unnamed field
/// is not.
pub fn validate_value(value: &SecretValue) -> Result<(), SecretsError> {
    let SecretValue::Fields(fields) = value else {
        return if value.is_empty() {
            Err(SecretsError::EmptyValue)
        } else {
            Ok(())
        };
    };
    if fields.is_empty() {
        return Err(SecretsError::EmptyFields);
    }
    if fields.len() > MAX_SECRET_FIELDS {
        return Err(SecretsError::InvalidMetadata(format!(
            "the credential has {} fields; at most {MAX_SECRET_FIELDS} are allowed",
            fields.len()
        )));
    }
    for key in fields.keys() {
        let shaped = (1..=MAX_SECRET_FIELD_KEY_LEN).contains(&key.len())
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if !shaped {
            return Err(SecretsError::InvalidMetadata(format!(
                "credential field name {key:?} must match \
                 [a-zA-Z0-9_-]{{1,{MAX_SECRET_FIELD_KEY_LEN}}}"
            )));
        }
    }
    Ok(())
}

/// A pin bounds where an opaque value may be sent. A `fields` credential
/// names its own upstream and no handler consults a pin for it, so one
/// offered with it would be accepted and never enforced.
pub fn validate_pin_fits(
    value: &SecretValue,
    allowed_hosts: &[String],
) -> Result<(), SecretsError> {
    if matches!(value, SecretValue::Fields(_)) && !allowed_hosts.is_empty() {
        return Err(SecretsError::InvalidMetadata(
            "allowedHosts applies to a value; a fields credential names its own upstream".into(),
        ));
    }
    Ok(())
}

pub fn validate_name(name: &str) -> Result<(), SecretsError> {
    if !is_valid_secret_name(name) {
        Err(SecretsError::InvalidName)
    } else if name.starts_with(SECRET_ID_PREFIX) {
        Err(SecretsError::ReservedName)
    } else {
        Ok(())
    }
}

/// Whether `token` has the shape of an identifier this service issues:
/// the prefix and 32 hex digits. A list cursor is such an identifier, so a
/// caller handing back anything else is refused rather than silently paged
/// from the start.
pub fn is_secret_id(token: &str) -> bool {
    token
        .strip_prefix(SECRET_ID_PREFIX)
        .is_some_and(|rest| rest.len() == 32 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub const MAX_ALLOWED_HOSTS: usize = 32;

/// Normalizes each pattern the way `rules` keys are normalized, so a pin and
/// a rule that name the same host agree on what that host is.
pub fn validate_allowed_hosts(allowed_hosts: Vec<String>) -> Result<Vec<String>, SecretsError> {
    if allowed_hosts.len() > MAX_ALLOWED_HOSTS {
        return Err(SecretsError::InvalidMetadata(format!(
            "allowedHosts carries {} entries; at most {MAX_ALLOWED_HOSTS} are allowed",
            allowed_hosts.len()
        )));
    }
    let mut normalized = Vec::with_capacity(allowed_hosts.len());
    for host in allowed_hosts {
        let pattern =
            crate::sandbox::network::policy::normalize_host_pattern(&host).ok_or_else(|| {
                SecretsError::InvalidMetadata(format!(
                    "allowedHosts entry {host:?} is not a DNS name or a single leading wildcard"
                ))
            })?;
        if !normalized.contains(&pattern) {
            normalized.push(pattern);
        }
    }
    Ok(normalized)
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
        Grant, SecretKind, SecretMetadata, SecretRef, SecretRefStore, SecretValue, SecretsBackend,
        SecretsError,
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
    /// One grant as the in-memory backend keeps it: the names and when it
    /// was recorded, keyed by `(sandbox_id, execution_id)`.
    type GrantRow = (Vec<String>, SystemTime);

    #[derive(Default)]
    pub struct InMemorySecretsBackend {
        values: Mutex<HashMap<String, (i64, SecretValue)>>,
        pins: Mutex<HashMap<String, Vec<String>>>,
        grants: Mutex<HashMap<(String, String), GrantRow>>,
        pub fail_puts: std::sync::atomic::AtomicBool,
    }

    impl InMemorySecretsBackend {
        pub fn version_of(&self, name: &str) -> Option<i64> {
            self.values.lock().unwrap().get(name).map(|(v, _)| *v)
        }

        pub fn pin_of(&self, name: &str) -> Option<Vec<String>> {
            self.pins.lock().unwrap().get(name).cloned()
        }

        pub fn grants_for(&self, sandbox_id: &str, execution_id: &str) -> Option<Vec<String>> {
            self.grants
                .lock()
                .unwrap()
                .get(&(sandbox_id.to_string(), execution_id.to_string()))
                .map(|(names, _)| names.clone())
        }

        /// Moves a grant's timestamp back, so a test can age it without waiting.
        pub fn backdate_grant(
            &self,
            sandbox_id: &str,
            execution_id: &str,
            by: std::time::Duration,
        ) {
            if let Some((_, granted_at)) = self
                .grants
                .lock()
                .unwrap()
                .get_mut(&(sandbox_id.to_string(), execution_id.to_string()))
            {
                *granted_at = granted_at.checked_sub(by).unwrap_or(SystemTime::UNIX_EPOCH);
            }
        }

        pub fn value_of(&self, name: &str) -> Option<SecretValue> {
            self.values
                .lock()
                .unwrap()
                .get(name)
                .map(|(_, value)| value.clone())
        }

        /// The opaque value under `name`, or `None` when it is absent or
        /// stored as fields.
        pub fn opaque_value_of(&self, name: &str) -> Option<String> {
            match self.value_of(name) {
                Some(SecretValue::Opaque(value)) => Some(value.expose().to_string()),
                _ => None,
            }
        }
    }

    #[async_trait]
    impl SecretsBackend for InMemorySecretsBackend {
        async fn put(
            &self,
            name: &str,
            value: &SecretValue,
            allowed_hosts: &[String],
        ) -> Result<i64, SecretsError> {
            if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SecretsError::Unavailable(anyhow::anyhow!(
                    "store refused the write"
                )));
            }
            let mut values = self.values.lock().unwrap();
            let version = values.get(name).map(|(v, _)| *v).unwrap_or(0) + 1;
            values.insert(name.to_string(), (version, value.clone()));
            self.pins
                .lock()
                .unwrap()
                .insert(name.to_string(), allowed_hosts.to_vec());
            Ok(version)
        }

        async fn delete(&self, name: &str) -> Result<(), SecretsError> {
            self.values.lock().unwrap().remove(name);
            self.pins.lock().unwrap().remove(name);
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
                (names.to_vec(), SystemTime::now()),
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

        async fn kinds_of(
            &self,
            names: &[String],
        ) -> Result<Option<BTreeMap<String, SecretKind>>, SecretsError> {
            let values = self.values.lock().unwrap();
            Ok(Some(
                names
                    .iter()
                    .filter_map(|name| Some((name.clone(), values.get(name)?.1.kind())))
                    .collect(),
            ))
        }

        async fn grants_before(
            &self,
            granted_before: SystemTime,
            limit: usize,
        ) -> Result<Vec<Grant>, SecretsError> {
            let grants = self.grants.lock().unwrap();
            let mut old: Vec<(SystemTime, Grant)> = grants
                .iter()
                .filter(|(_, (_, granted_at))| *granted_at < granted_before)
                .map(|((sandbox_id, execution_id), (_, granted_at))| {
                    (
                        *granted_at,
                        Grant {
                            sandbox_id: sandbox_id.clone(),
                            execution_id: execution_id.clone(),
                        },
                    )
                })
                .collect();
            old.sort_by_key(|(granted_at, _)| *granted_at);
            Ok(old
                .into_iter()
                .map(|(_, grant)| grant)
                .take(limit)
                .collect())
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

    fn value(s: &str) -> SecretValue {
        SecretValue::Opaque(SecretString::new(s.to_string()))
    }

    fn fields(pairs: &[(&str, &str)]) -> SecretValue {
        SecretValue::Fields(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), SecretString::new(value.to_string())))
                .collect(),
        )
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
                .create("openai", value("sk"), SecretMetadata::new(), Vec::new())
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
            .create("gh", value("v1"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        refs.vanish.store(true, Ordering::SeqCst);

        assert!(matches!(
            service.update("gh", value("v2"), None, Vec::new()).await,
            Err(SecretsError::NotFound)
        ));
        assert!(
            backend.value_of("gh").is_none(),
            "the version this call wrote must not outlive the row it belonged to"
        );
    }

    #[test]
    fn a_value_is_redacted_in_both_of_its_shapes() {
        let secret = SecretString::new("sk-live-123".to_string());
        assert_eq!(format!("{secret:?}"), "SecretString([redacted])");
        assert_eq!(secret.expose(), "sk-live-123");

        for shaped in [value("sk-live-123"), fields(&[("password", "sk-live-123")])] {
            assert!(!format!("{shaped:?}").contains("sk-live-123"), "{shaped:?}");
        }
    }

    #[tokio::test]
    async fn create_stores_the_value_in_the_backend_and_the_row_without_it() {
        let (service, backend) = service();
        let created = service
            .create("openai", value("sk"), SecretMetadata::new(), Vec::new())
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
            .create("openai", value("sk"), SecretMetadata::new(), Vec::new())
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
        for bad in ["", "with/slash", &"a".repeat(129)] {
            assert!(matches!(
                service
                    .create(bad, value("v"), SecretMetadata::new(), Vec::new())
                    .await,
                Err(SecretsError::InvalidName)
            ));
        }
        assert!(
            matches!(
                service
                    .create(
                        "sec_reserved",
                        value("v"),
                        SecretMetadata::new(),
                        Vec::new()
                    )
                    .await,
                Err(SecretsError::ReservedName)
            ),
            "a well-formed name in the identifier namespace is refused for that reason"
        );
        assert!(matches!(
            service
                .create("empty", value(""), SecretMetadata::new(), Vec::new())
                .await,
            Err(SecretsError::EmptyValue)
        ));
        assert!(is_secret_id(&new_secret_id()));
        for not_an_id in [
            "garbage",
            "sec_",
            "sec_xyz",
            "SEC_0123456789abcdef0123456789abcdef",
            "",
        ] {
            assert!(!is_secret_id(not_an_id), "{not_an_id:?}");
        }
        service
            .create("gh", value("v"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        assert!(matches!(
            service.create("gh", value("v2"), SecretMetadata::new(), Vec::new()).await,
            Err(SecretsError::AlreadyExists(name)) if name == "gh"
        ));
    }

    #[tokio::test]
    async fn update_appends_a_version_and_delete_removes_value_and_row() {
        let (service, backend) = service();
        let created = service
            .create("gh", value("v1"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        let mut metadata = SecretMetadata::new();
        metadata.insert("owner".into(), "team-a".into());
        let updated = service
            .update(
                &created.secret_id,
                value("v2"),
                Some(metadata.clone()),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(updated.current_version, 2);
        assert_eq!(updated.metadata, metadata);
        assert_eq!(backend.opaque_value_of("gh").as_deref(), Some("v2"));

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
    async fn a_structured_credential_round_trips_and_its_field_names_are_checked() {
        let (service, backend) = service();
        let created = service
            .create(
                "tenant_db",
                fields(&[("host", "pg.internal"), ("port", "5432"), ("password", "")]),
                SecretMetadata::new(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(created.current_version, 1);
        match backend.value_of("tenant_db").unwrap() {
            SecretValue::Fields(stored) => {
                assert_eq!(stored.len(), 3);
                assert_eq!(stored["host"].expose(), "pg.internal");
                assert_eq!(
                    stored["password"].expose(),
                    "",
                    "an empty password is a real configuration; only an unnamed field is not"
                );
            }
            SecretValue::Opaque(_) => panic!("fields were stored as an opaque value"),
        }

        assert!(matches!(
            service
                .create("empty", fields(&[]), SecretMetadata::new(), Vec::new())
                .await,
            Err(SecretsError::EmptyFields)
        ));
        for bad in [
            "",
            "with space",
            "with/slash",
            &"k".repeat(MAX_SECRET_FIELD_KEY_LEN + 1),
        ] {
            assert!(
                matches!(
                    service
                        .create(
                            "db",
                            fields(&[(bad, "v")]),
                            SecretMetadata::new(),
                            Vec::new()
                        )
                        .await,
                    Err(SecretsError::InvalidMetadata(_))
                ),
                "{bad:?} was accepted as a field name"
            );
        }
        let many: Vec<(String, String)> = (0..=MAX_SECRET_FIELDS)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        assert!(matches!(
            service
                .create(
                    "db",
                    SecretValue::Fields(
                        many.iter()
                            .map(|(k, v)| (k.clone(), SecretString::new(v.clone())))
                            .collect()
                    ),
                    SecretMetadata::new(),
                    Vec::new(),
                )
                .await,
            Err(SecretsError::InvalidMetadata(_))
        ));
    }

    #[tokio::test]
    async fn listing_pages_by_id_with_a_cursor() {
        let (service, _) = service();
        for name in ["a", "b", "c"] {
            service
                .create(name, value("v"), SecretMetadata::new(), Vec::new())
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
        async fn put(&self, _: &str, _: &SecretValue, _: &[String]) -> Result<i64, SecretsError> {
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
        async fn kinds_of(
            &self,
            names: &[String],
        ) -> Result<Option<BTreeMap<String, SecretKind>>, SecretsError> {
            Ok(Some(
                names
                    .iter()
                    .filter(|name| self.known.contains(name))
                    .map(|name| (name.clone(), SecretKind::Opaque))
                    .collect(),
            ))
        }
    }

    fn opaque_uses(names: &[&str]) -> BTreeMap<String, SecretKind> {
        names
            .iter()
            .map(|name| (name.to_string(), SecretKind::Opaque))
            .collect()
    }

    #[tokio::test]
    async fn a_pin_is_normalized_the_way_a_rules_key_is_and_travels_with_the_value() {
        let (service, backend) = service();
        service
            .create(
                "openai",
                value("sk"),
                SecretMetadata::new(),
                vec![
                    "API.OpenAI.com".into(),
                    "*.GitHub.com".into(),
                    "api.openai.com".into(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            backend.pin_of("openai"),
            Some(vec![
                "api.openai.com".to_string(),
                "*.github.com".to_string()
            ]),
            "patterns are lowercased and deduplicated, as rules keys are"
        );

        // A new version replaces the pin: it lives with the value.
        service
            .update("openai", value("sk2"), None, Vec::new())
            .await
            .unwrap();
        assert_eq!(backend.pin_of("openai"), Some(Vec::new()));
    }

    #[tokio::test]
    async fn a_pin_that_is_not_a_host_pattern_is_refused_before_anything_is_written() {
        let (service, backend) = service();
        for bad in ["not a host", "*.", "*", "http://api.example.com", ""] {
            let err = service
                .create(
                    "openai",
                    value("sk"),
                    SecretMetadata::new(),
                    vec![bad.into()],
                )
                .await
                .err()
                .unwrap();
            assert!(
                matches!(err, SecretsError::InvalidMetadata(_)),
                "{bad:?} gave {err:?}"
            );
        }
        let many: Vec<String> = (0..=MAX_ALLOWED_HOSTS)
            .map(|i| format!("h{i}.example.com"))
            .collect();
        assert!(matches!(
            service
                .create("openai", value("sk"), SecretMetadata::new(), many)
                .await,
            Err(SecretsError::InvalidMetadata(_))
        ));
        assert_eq!(backend.version_of("openai"), None);
    }

    #[tokio::test]
    async fn a_backend_that_owns_its_names_answers_instead_of_the_ref_store() {
        let service = SecretsService::new(
            Arc::new(InMemorySecretRefStore::default()),
            Arc::new(NameOwningBackend {
                known: vec!["tenant_db".to_string()],
            }),
        );
        // The ref store holds no row for either name, so an answer that only
        // consulted it would call both of them missing.
        service
            .ensure_usable(&opaque_uses(&["tenant_db"]))
            .await
            .unwrap();
        let err = service
            .ensure_usable(&opaque_uses(&["tenant_db", "absent"]))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, UnusableSecrets::Missing(missing) if missing == ["absent"]));
    }

    #[tokio::test]
    async fn ensure_usable_reports_only_the_missing_ones() {
        let (service, _) = service();
        service
            .create("present", value("v"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        match service
            .ensure_usable(&opaque_uses(&["present", "absent"]))
            .await
        {
            Err(UnusableSecrets::Missing(missing)) => assert_eq!(missing, vec!["absent"]),
            other => panic!("expected the missing name, got {other:?}"),
        }
        assert!(service
            .ensure_usable(&opaque_uses(&["present"]))
            .await
            .is_ok());
        assert!(service.ensure_usable(&BTreeMap::new()).await.is_ok());
    }

    #[tokio::test]
    async fn a_name_used_in_the_other_shape_is_unusable_before_placement() {
        let (service, _) = service();
        service
            .create("openai", value("sk"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        service
            .create(
                "tenant_db",
                fields(&[("host", "pg.internal")]),
                SecretMetadata::new(),
                Vec::new(),
            )
            .await
            .unwrap();

        let mut wanted = BTreeMap::new();
        wanted.insert("openai".to_string(), SecretKind::Fields);
        wanted.insert("tenant_db".to_string(), SecretKind::Opaque);
        match service.ensure_usable(&wanted).await {
            Err(UnusableSecrets::WrongShape(mismatched)) => assert_eq!(
                mismatched,
                vec![
                    ShapeMismatch {
                        name: "openai".into(),
                        stored: SecretKind::Opaque,
                        needed: SecretKind::Fields,
                    },
                    ShapeMismatch {
                        name: "tenant_db".into(),
                        stored: SecretKind::Fields,
                        needed: SecretKind::Opaque,
                    },
                ]
            ),
            other => panic!("expected both shapes to be refused, got {other:?}"),
        }
        assert_eq!(
            service.unusable_names(&wanted).await.unwrap().len(),
            2,
            "a wake reports a wrong shape the way it reports a missing name"
        );

        let mut right = BTreeMap::new();
        right.insert("openai".to_string(), SecretKind::Opaque);
        right.insert("tenant_db".to_string(), SecretKind::Fields);
        assert!(service.ensure_usable(&right).await.is_ok());
    }

    #[tokio::test]
    async fn a_new_version_keeps_the_shape_the_secret_was_created_with() {
        let (service, backend) = service();
        service
            .create("openai", value("sk"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        let err = service
            .update("openai", fields(&[("password", "sk")]), None, Vec::new())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                &err,
                SecretsError::ShapeChanged { name, stored: SecretKind::Opaque, offered: SecretKind::Fields }
                    if name == "openai"
            ),
            "{err:?}"
        );
        assert_eq!(
            backend.version_of("openai"),
            Some(1),
            "a refused version leaves the served one alone"
        );
        service
            .update("openai", value("sk2"), None, Vec::new())
            .await
            .unwrap();
        assert_eq!(backend.version_of("openai"), Some(2));
    }

    #[tokio::test]
    async fn a_pin_is_refused_on_a_fields_credential() {
        let (service, backend) = service();
        let err = service
            .create(
                "tenant_db",
                fields(&[("host", "pg-a.internal")]),
                SecretMetadata::new(),
                vec!["pg-b.internal".into()],
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SecretsError::InvalidMetadata(_)), "{err:?}");
        assert_eq!(backend.version_of("tenant_db"), None);

        service
            .create(
                "tenant_db",
                fields(&[("host", "pg-a.internal")]),
                SecretMetadata::new(),
                Vec::new(),
            )
            .await
            .unwrap();
        let err = service
            .update(
                "tenant_db",
                fields(&[("host", "pg-a.internal")]),
                None,
                vec!["pg-b.internal".into()],
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SecretsError::InvalidMetadata(_)), "{err:?}");
        assert_eq!(backend.version_of("tenant_db"), Some(1));
    }

    #[tokio::test]
    async fn only_grants_older_than_the_horizon_are_reaper_candidates() {
        let (service, backend) = service();
        let old_sandbox = SandboxId::new();
        let old_execution = ExecutionId::new();
        let fresh_sandbox = SandboxId::new();
        let fresh_execution = ExecutionId::new();
        let names: BTreeSet<String> = ["openai".to_string()].into_iter().collect();
        service
            .grant(old_sandbox, old_execution, &names)
            .await
            .unwrap();
        service
            .grant(fresh_sandbox, fresh_execution, &names)
            .await
            .unwrap();
        backend.backdate_grant(
            &old_sandbox.to_string(),
            &old_execution.to_string(),
            Duration::from_secs(3600),
        );

        assert_eq!(
            service
                .stale_grant_candidates(Duration::from_secs(600), 10)
                .await
                .unwrap(),
            vec![(old_sandbox, old_execution)]
        );
        assert!(service
            .stale_grant_candidates(Duration::from_secs(7200), 10)
            .await
            .unwrap()
            .is_empty());
    }

    // Nothing here ties a name to an owner: the isolation axis is the
    // control-plane credential.
    #[tokio::test]
    async fn any_name_that_exists_can_be_granted_to_any_sandbox() {
        let (service, backend) = service();
        for name in ["tenant_db_ws42", "tenant_db_ws99"] {
            service
                .create(name, value("dsn"), SecretMetadata::new(), Vec::new())
                .await
                .unwrap();
        }
        let sandbox = SandboxId::new();
        let execution = ExecutionId::new();
        let both: BTreeSet<String> = ["tenant_db_ws42", "tenant_db_ws99"]
            .into_iter()
            .map(String::from)
            .collect();

        service.grant(sandbox, execution, &both).await.unwrap();

        assert_eq!(
            backend
                .grants_for(&sandbox.to_string(), &execution.to_string())
                .unwrap(),
            vec!["tenant_db_ws42".to_string(), "tenant_db_ws99".to_string()],
            "nothing here ties a name to an owner; the prefix is a naming \
             discipline for audit, not a check"
        );
    }

    #[tokio::test]
    async fn a_store_that_cannot_answer_is_not_an_all_present_answer() {
        let (service, _) = service();
        service
            .create("present", value("v"), SecretMetadata::new(), Vec::new())
            .await
            .unwrap();
        let names = opaque_uses(&["present", "absent"]);
        assert_eq!(
            service.unusable_names(&names).await,
            Some(vec!["absent".to_string()])
        );

        let broken = SecretsService::new(
            Arc::new(InMemorySecretRefStore::default()),
            Arc::new(UnavailableBackend),
        );
        assert_eq!(
            broken.unusable_names(&names).await,
            None,
            "an outage must not read as \"every name is present\""
        );
    }

    /// A backend that owns its names and cannot reach them.
    struct UnavailableBackend;

    #[async_trait]
    impl SecretsBackend for UnavailableBackend {
        async fn put(&self, _: &str, _: &SecretValue, _: &[String]) -> Result<i64, SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!("down")))
        }
        async fn delete(&self, _: &str) -> Result<(), SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!("down")))
        }
        async fn grant(&self, _: &str, _: &str, _: &[String]) -> Result<(), SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!("down")))
        }
        async fn revoke(&self, _: &str, _: &str) -> Result<(), SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!("down")))
        }
        async fn kinds_of(
            &self,
            _: &[String],
        ) -> Result<Option<BTreeMap<String, SecretKind>>, SecretsError> {
            Err(SecretsError::Unavailable(anyhow::anyhow!("down")))
        }
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
