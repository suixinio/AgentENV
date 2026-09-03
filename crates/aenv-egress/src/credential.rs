use std::collections::HashMap;
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
}

impl Secret {
    pub fn new(value: Vec<u8>, expires_at: Option<SystemTime>) -> Self {
        Self {
            value: Zeroizing::new(value),
            expires_at,
        }
    }

    pub fn expose(&self) -> &[u8] {
        &self.value
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secret")
            .field("value", &"[redacted]")
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
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError>;
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
}

/// Fixed (sandbox, execution, name) -> value entries, for tests and embedded
/// smoke runs.
#[derive(Default)]
pub struct StaticSource {
    entries: HashMap<(String, String, String), Vec<u8>>,
}

impl StaticSource {
    pub fn with(mut self, sandbox_id: &str, execution_id: &str, name: &str, value: &[u8]) -> Self {
        self.entries.insert(
            (sandbox_id.into(), execution_id.into(), name.into()),
            value.to_vec(),
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
            .map(|value| Secret::new(value.clone(), None))
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
        }
    }

    /// A capacity of zero is raised to one: the map always admits the entry
    /// it was asked to insert.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    fn make_room(&self, cache: &mut HashMap<GrantKey, (Secret, Instant)>, now: Instant) {
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

    fn expiry_for(&self, secret: &Secret, now: Instant) -> Instant {
        let by_ttl = now + self.ttl;
        match secret.expires_at() {
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
        let expires_at = self.expiry_for(&secret, now);
        let mut cache = self.cache.lock().await;
        self.make_room(&mut cache, Instant::now());
        cache.insert(key, (secret.clone(), expires_at));
        Ok(secret)
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

    #[tokio::test]
    async fn the_static_source_answers_only_its_own_tuples() {
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
