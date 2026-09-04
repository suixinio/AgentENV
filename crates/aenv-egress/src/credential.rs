use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::Instant;
use zeroize::Zeroizing;

/// Names accepted on both the defining (`/secrets`) and referencing
/// (`${aenv.secrets.NAME}`) side: `^[a-zA-Z0-9_-]{1,128}$`.
pub fn is_valid_secret_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A credential value that is wiped on drop and never printed.
#[derive(Clone)]
pub struct Secret {
    value: Zeroizing<Vec<u8>>,
    expires_at: Option<SystemTime>,
    allowed_hosts: Vec<String>,
}

impl Secret {
    pub fn new(value: Vec<u8>, expires_at: Option<SystemTime>) -> Self {
        Self {
            value: Zeroizing::new(value),
            expires_at,
            allowed_hosts: Vec::new(),
        }
    }

    /// Pins this value to the hosts it may be sent to. An empty list is
    /// unpinned: the rule that named it is then the only bound.
    pub fn with_allowed_hosts(mut self, allowed_hosts: Vec<String>) -> Self {
        self.allowed_hosts = allowed_hosts;
        self
    }

    pub fn expose(&self) -> &[u8] {
        &self.value
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    pub fn allowed_hosts(&self) -> &[String] {
        &self.allowed_hosts
    }

    /// Whether this value may be sent to `host`. Patterns are the ones
    /// `rules` keys use: an exact name, or one leading `*.` wildcard that
    /// matches any depth below it and never the apex.
    pub fn may_reach(&self, host: &str) -> bool {
        if self.allowed_hosts.is_empty() {
            return true;
        }
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.allowed_hosts.iter().any(|pattern| {
            let pattern = pattern.trim_end_matches('.').to_ascii_lowercase();
            match pattern.strip_prefix("*.") {
                Some(suffix) => {
                    host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}"))
                }
                None => host == pattern,
            }
        })
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secret")
            .field("value", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field("allowed_hosts", &self.allowed_hosts)
            .finish()
    }
}

/// A credential made of named fields rather than one value: what a protocol
/// handler binds to a connection. A field the source leaves out is a field
/// the handler passes through unchanged, so absence is meaningful and an
/// empty string is not the same as absent.
#[derive(Clone, Default)]
pub struct CredentialFields {
    fields: BTreeMap<String, Zeroizing<String>>,
    expires_at: Option<SystemTime>,
}

impl CredentialFields {
    pub fn new(fields: BTreeMap<String, String>, expires_at: Option<SystemTime>) -> Self {
        Self {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name, Zeroizing::new(value)))
                .collect(),
            expires_at,
        }
    }

    /// Reads a JSON object, coercing each scalar to its string form so a
    /// resolver may write a port as a number. A nested value is refused
    /// rather than stringified into something the handler would misread.
    pub fn from_json(
        object: &serde_json::Map<String, serde_json::Value>,
        expires_at: Option<SystemTime>,
    ) -> Result<Self, CredentialError> {
        let mut fields = BTreeMap::new();
        for (name, value) in object {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Number(value) => value.to_string(),
                serde_json::Value::Bool(value) => value.to_string(),
                serde_json::Value::Null => continue,
                _ => {
                    return Err(CredentialError::Unavailable(format!(
                        "credential field {name:?} is not a scalar"
                    )))
                }
            };
            fields.insert(name.clone(), value);
        }
        Ok(Self::new(fields, expires_at))
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(|value| value.as_str())
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

impl fmt::Debug for CredentialFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialFields")
            .field("names", &self.fields.keys().collect::<Vec<_>>())
            .field("values", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    /// No grant covers this (sandbox, execution, name), or the name is invalid.
    #[error("credential access denied")]
    Denied,
    /// The source could not answer; the caller reports the upstream as
    /// unavailable, never as denied.
    #[error("credential source unavailable: {0}")]
    Unavailable(String),
}

impl CredentialError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Denied => "credential_denied",
            Self::Unavailable(_) => "credential_unavailable",
        }
    }
}

#[async_trait]
pub trait CredentialSource: Send + Sync {
    /// One opaque value, for a handler that substitutes it into a message.
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError>;

    /// The same grant read as named fields, for a handler that binds a
    /// credential to a connection. A source with no structured form reports
    /// itself unavailable rather than inventing one.
    async fn get_fields(
        &self,
        _sandbox_id: &str,
        _execution_id: &str,
        _name: &str,
    ) -> Result<CredentialFields, CredentialError> {
        Err(CredentialError::Unavailable(
            "this credential source returns opaque values only".into(),
        ))
    }
}

/// The source of a deployment with no store: every lookup is unavailable.
pub struct NoCredentials;

#[async_trait]
impl CredentialSource for NoCredentials {
    async fn get(&self, _: &str, _: &str, _: &str) -> Result<Secret, CredentialError> {
        Err(CredentialError::Unavailable(
            "no credential source configured".into(),
        ))
    }

    async fn get_fields(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<CredentialFields, CredentialError> {
        Err(CredentialError::Unavailable(
            "no credential source configured".into(),
        ))
    }
}

/// Fixed (sandbox, execution, name) -> value entries, for tests and embedded
/// smoke runs.
#[derive(Default)]
pub struct StaticSource {
    entries: HashMap<GrantKey, (Vec<u8>, Vec<String>)>,
    fields: HashMap<GrantKey, BTreeMap<String, String>>,
}

impl StaticSource {
    pub fn with(self, sandbox_id: &str, execution_id: &str, name: &str, value: &[u8]) -> Self {
        self.with_pinned(sandbox_id, execution_id, name, value, &[])
    }

    pub fn with_pinned(
        mut self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
        value: &[u8],
        allowed_hosts: &[&str],
    ) -> Self {
        self.entries.insert(
            (sandbox_id.into(), execution_id.into(), name.into()),
            (
                value.to_vec(),
                allowed_hosts.iter().map(|host| host.to_string()).collect(),
            ),
        );
        self
    }

    pub fn with_fields(
        mut self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
        fields: &[(&str, &str)],
    ) -> Self {
        self.fields.insert(
            (sandbox_id.into(), execution_id.into(), name.into()),
            fields
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        );
        self
    }
}

#[async_trait]
impl CredentialSource for StaticSource {
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError> {
        self.entries
            .get(&(sandbox_id.into(), execution_id.into(), name.into()))
            .map(|(value, allowed_hosts)| {
                Secret::new(value.clone(), None).with_allowed_hosts(allowed_hosts.clone())
            })
            .ok_or(CredentialError::Denied)
    }

    async fn get_fields(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<CredentialFields, CredentialError> {
        self.fields
            .get(&(sandbox_id.into(), execution_id.into(), name.into()))
            .map(|fields| CredentialFields::new(fields.clone(), None))
            .ok_or(CredentialError::Denied)
    }
}

/// Caches successful lookups for `ttl`, or until the secret's own expiry if
/// that is sooner, holding at most `capacity` entries and dropping expired
/// ones on insert. Denials, outages and invalid names never reach the cache.
pub struct CachingSource<S> {
    inner: S,
    ttl: Duration,
    capacity: usize,
    cache: Mutex<HashMap<GrantKey, (Secret, Instant)>>,
    fields_cache: Mutex<HashMap<GrantKey, (CredentialFields, Instant)>>,
}

type GrantKey = (String, String, String);

/// Entries a broker holds when the operator names no capacity.
pub const DEFAULT_CREDENTIAL_CACHE_CAPACITY: usize = 4096;

impl<S: CredentialSource> CachingSource<S> {
    pub fn new(inner: S, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            capacity: DEFAULT_CREDENTIAL_CACHE_CAPACITY,
            cache: Mutex::new(HashMap::new()),
            fields_cache: Mutex::new(HashMap::new()),
        }
    }

    /// A capacity of zero is raised to one: the map always admits the entry
    /// it was asked to insert.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    fn make_room<V>(&self, cache: &mut HashMap<GrantKey, (V, Instant)>, now: Instant) {
        cache.retain(|_, (_, expires_at)| *expires_at > now);
        while cache.len() >= self.capacity {
            let Some(soonest) = cache
                .iter()
                .min_by_key(|(_, (_, expires_at))| *expires_at)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            cache.remove(&soonest);
        }
    }

    fn expiry_for(&self, expires_at: Option<SystemTime>, now: Instant) -> Instant {
        let by_ttl = now + self.ttl;
        match expires_at {
            Some(at) => {
                let remaining = at
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO);
                by_ttl.min(now + remaining)
            }
            None => by_ttl,
        }
    }
}

#[async_trait]
impl<S: CredentialSource> CredentialSource for CachingSource<S> {
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError> {
        if !is_valid_secret_name(name) {
            return Err(CredentialError::Denied);
        }
        let key = (
            sandbox_id.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        let now = Instant::now();
        {
            let mut cache = self.cache.lock().await;
            match cache.get(&key) {
                Some((secret, expires_at)) if *expires_at > now => return Ok(secret.clone()),
                Some(_) => {
                    cache.remove(&key);
                }
                None => {}
            }
        }
        let secret = self.inner.get(sandbox_id, execution_id, name).await?;
        let expires_at = self.expiry_for(secret.expires_at(), now);
        let mut cache = self.cache.lock().await;
        self.make_room(&mut cache, Instant::now());
        cache.insert(key, (secret.clone(), expires_at));
        Ok(secret)
    }

    async fn get_fields(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<CredentialFields, CredentialError> {
        if !is_valid_secret_name(name) {
            return Err(CredentialError::Denied);
        }
        let key = (
            sandbox_id.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        let now = Instant::now();
        {
            let mut cache = self.fields_cache.lock().await;
            match cache.get(&key) {
                Some((fields, expires_at)) if *expires_at > now => return Ok(fields.clone()),
                Some(_) => {
                    cache.remove(&key);
                }
                None => {}
            }
        }
        let fields = self
            .inner
            .get_fields(sandbox_id, execution_id, name)
            .await?;
        let expires_at = self.expiry_for(fields.expires_at(), now);
        let mut cache = self.fields_cache.lock().await;
        self.make_room(&mut cache, Instant::now());
        cache.insert(key, (fields.clone(), expires_at));
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn secret_debug_never_shows_the_value() {
        let secret = Secret::new(b"sk-live-very-secret".to_vec(), None);
        let printed = format!("{secret:?}");
        assert!(!printed.contains("sk-live"));
        assert!(printed.contains("[redacted]"));
        assert_eq!(secret.expose(), b"sk-live-very-secret");
    }

    #[test]
    fn an_unpinned_secret_reaches_anything_and_a_pinned_one_reaches_its_patterns() {
        let unpinned = Secret::new(b"v".to_vec(), None);
        assert!(unpinned.may_reach("anywhere.example"));

        let pinned = Secret::new(b"v".to_vec(), None)
            .with_allowed_hosts(vec!["api.openai.com".into(), "*.github.com".into()]);
        assert!(pinned.may_reach("api.openai.com"));
        assert!(pinned.may_reach("API.OpenAI.com"));
        assert!(pinned.may_reach("api.openai.com."));
        assert!(pinned.may_reach("codeload.github.com"));
        assert!(pinned.may_reach("a.b.github.com"));

        assert!(
            !pinned.may_reach("github.com"),
            "a wildcard never matches the apex"
        );
        assert!(!pinned.may_reach("evil.com"));
        assert!(!pinned.may_reach("api.openai.com.evil.com"));
        assert!(!pinned.may_reach("notapi.openai.com"));
        assert!(!pinned.may_reach("xgithub.com"));
    }

    #[test]
    fn secret_names_follow_the_shared_grammar() {
        assert!(is_valid_secret_name("openai"));
        assert!(is_valid_secret_name("GH_token-2"));
        assert!(is_valid_secret_name(&"a".repeat(128)));
        assert!(!is_valid_secret_name(""));
        assert!(!is_valid_secret_name(&"a".repeat(129)));
        assert!(!is_valid_secret_name("with/slash"));
        assert!(!is_valid_secret_name("../escape"));
        assert!(!is_valid_secret_name("has space"));
        assert!(!is_valid_secret_name("dotted.name"));
    }

    struct Counting {
        calls: Arc<AtomicUsize>,
        answer: Result<Vec<u8>, CredentialError>,
    }

    #[async_trait]
    impl CredentialSource for Counting {
        async fn get(&self, _: &str, _: &str, _: &str) -> Result<Secret, CredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.answer.clone().map(|value| Secret::new(value, None))
        }
    }

    fn counting(answer: Result<Vec<u8>, CredentialError>) -> (Counting, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Counting {
                calls: calls.clone(),
                answer,
            },
            calls,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_hit_is_served_from_cache_until_the_ttl_passes() {
        let (inner, calls) = counting(Ok(b"v1".to_vec()));
        let source = CachingSource::new(inner, Duration::from_secs(60));

        assert_eq!(source.get("s", "e", "n").await.unwrap().expose(), b"v1");
        assert_eq!(source.get("s", "e", "n").await.unwrap().expose(), b"v1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(61)).await;
        source.get("s", "e", "n").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_cache_key_is_the_full_grant_tuple() {
        let (inner, calls) = counting(Ok(b"v1".to_vec()));
        let source = CachingSource::new(inner, Duration::from_secs(60));

        source.get("s", "e1", "n").await.unwrap();
        source.get("s", "e2", "n").await.unwrap();
        source.get("s2", "e1", "n").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn denials_and_outages_are_not_cached() {
        let (inner, calls) = counting(Err(CredentialError::Denied));
        let source = CachingSource::new(inner, Duration::from_secs(60));
        assert_eq!(
            source.get("s", "e", "n").await.err(),
            Some(CredentialError::Denied)
        );
        assert_eq!(
            source.get("s", "e", "n").await.err(),
            Some(CredentialError::Denied)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let (inner, calls) = counting(Err(CredentialError::Unavailable("down".into())));
        let source = CachingSource::new(inner, Duration::from_secs(60));
        assert!(matches!(
            source.get("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
        assert!(matches!(
            source.get("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_invalid_name_is_denied_without_reaching_the_inner_source() {
        let (inner, calls) = counting(Ok(b"v1".to_vec()));
        let source = CachingSource::new(inner, Duration::from_secs(60));
        assert_eq!(
            source.get("s", "e", "../other").await.err(),
            Some(CredentialError::Denied)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_entry_leaves_the_cache_without_a_lookup_of_its_own_key() {
        let (inner, _) = counting(Ok(b"v1".to_vec()));
        let source = CachingSource::new(inner, Duration::from_secs(60));
        source.get("s", "e", "n").await.unwrap();

        tokio::time::advance(Duration::from_secs(61)).await;
        source.get("s", "other-exec", "n").await.unwrap();

        let cache = source.cache.lock().await;
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains_key(&("s".into(), "e".into(), "n".into())));
    }

    #[tokio::test(start_paused = true)]
    async fn the_cache_holds_no_more_entries_than_its_capacity() {
        let (inner, _) = counting(Ok(b"v1".to_vec()));
        let source = CachingSource::new(inner, Duration::from_secs(60)).with_capacity(2);
        for execution in ["e1", "e2", "e3"] {
            source.get("s", execution, "n").await.unwrap();
        }
        assert_eq!(source.cache.lock().await.len(), 2);
    }

    struct CountingFields {
        calls: Arc<AtomicUsize>,
        expires_at: Option<SystemTime>,
    }

    #[async_trait]
    impl CredentialSource for CountingFields {
        async fn get(&self, _: &str, _: &str, _: &str) -> Result<Secret, CredentialError> {
            Err(CredentialError::Denied)
        }

        async fn get_fields(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<CredentialFields, CredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(CredentialFields::new(
                BTreeMap::from([("user".to_string(), "rw_app".to_string())]),
                self.expires_at,
            ))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fields_are_cached_under_their_own_key_and_do_not_answer_an_opaque_lookup() {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = CachingSource::new(
            CountingFields {
                calls: calls.clone(),
                expires_at: None,
            },
            Duration::from_secs(60),
        );

        assert_eq!(
            source.get_fields("s", "e", "n").await.unwrap().get("user"),
            Some("rw_app")
        );
        source.get_fields("s", "e", "n").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // The opaque form is a different question with a different answer.
        assert_eq!(
            source.get("s", "e", "n").await.err(),
            Some(CredentialError::Denied)
        );

        tokio::time::advance(Duration::from_secs(61)).await;
        source.get_fields("s", "e", "n").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_credential_that_states_its_own_expiry_is_not_held_past_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = CachingSource::new(
            CountingFields {
                calls: calls.clone(),
                expires_at: Some(SystemTime::now() + Duration::from_secs(5)),
            },
            Duration::from_secs(600),
        );

        source.get_fields("s", "e", "n").await.unwrap();
        tokio::time::advance(Duration::from_secs(6)).await;
        source.get_fields("s", "e", "n").await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the ttl must not outlive the credential's own expiry"
        );
    }

    #[test]
    fn a_field_set_reads_scalars_and_refuses_anything_deeper() {
        let object = serde_json::json!({
            "host": "db.internal",
            "port": 5432,
            "sslmode": false,
            "database": null,
        });
        let fields = CredentialFields::from_json(object.as_object().unwrap(), None).unwrap();
        assert_eq!(fields.get("host"), Some("db.internal"));
        assert_eq!(fields.get("port"), Some("5432"));
        assert_eq!(fields.get("sslmode"), Some("false"));
        assert_eq!(fields.get("database"), None, "null is absent, not empty");
        assert!(fields.names().eq(["host", "port", "sslmode"]));

        let nested = serde_json::json!({"host": {"name": "db"}});
        assert!(matches!(
            CredentialFields::from_json(nested.as_object().unwrap(), None),
            Err(CredentialError::Unavailable(_))
        ));
    }

    #[test]
    fn a_field_set_never_prints_its_values() {
        let fields = CredentialFields::new(
            BTreeMap::from([("password".to_string(), "hunter2".to_string())]),
            None,
        );
        let printed = format!("{fields:?}");
        assert!(!printed.contains("hunter2"));
        assert!(printed.contains("password"));
        assert!(printed.contains("[redacted]"));
    }

    #[tokio::test]
    async fn a_source_with_no_structured_form_is_unavailable_not_denied() {
        let (inner, _) = counting(Ok(b"v1".to_vec()));
        assert!(matches!(
            inner.get_fields("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn the_static_source_answers_only_its_own_tuples() {
        let fields = StaticSource::default().with_fields("s", "e", "db", &[("user", "rw")]);
        assert_eq!(
            fields.get_fields("s", "e", "db").await.unwrap().get("user"),
            Some("rw")
        );
        assert_eq!(
            fields.get_fields("s", "other-exec", "db").await.err(),
            Some(CredentialError::Denied)
        );

        let source = StaticSource::default().with("s", "e", "openai", b"sk");
        assert_eq!(
            source.get("s", "e", "openai").await.unwrap().expose(),
            b"sk"
        );
        assert_eq!(
            source.get("s", "other-exec", "openai").await.err(),
            Some(CredentialError::Denied)
        );
        assert!(matches!(
            NoCredentials.get("s", "e", "openai").await,
            Err(CredentialError::Unavailable(_))
        ));
    }
}
