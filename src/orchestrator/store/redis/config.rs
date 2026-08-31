use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default namespace, disjoint from routing projections.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:api";

/// Redis metadata-store configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisStoreConfig {
    /// `redis://host:port[/db]`.
    pub url: String,
    /// Prefix for every key this store owns.
    pub key_prefix: String,

    /// Per-sandbox lock TTL.
    pub lock_ttl: Duration,
    /// Maximum lock acquisition wait, independent from lock TTL.
    pub lock_wait: Duration,
    pub lock_retry_min: Duration,
    pub lock_retry_max: Duration,
    /// Fraction of the backoff to jitter by, in either direction.
    pub lock_retry_jitter: f64,
    /// How long the synchronous update callback may run before its result is
    /// discarded rather than written.
    pub closure_budget: Duration,
    /// Headroom the write itself is assumed to need. Checked against the
    /// lock's remaining lifetime before the write is attempted.
    pub write_budget: Duration,

    /// Grace keeping records alive beyond the sandbox lifetime ceiling.
    pub record_ttl_grace: Duration,

    pub transition_key_ttl: Duration,
    pub transition_result_ttl: Duration,
    /// Caller-side transition wait used to validate timeout ordering.
    pub wait_transition_timeout: Duration,
    /// Bounded retries after waiting for in-flight transitions.
    pub max_transition_retries: u32,

    pub expired_batch_limit: usize,
    /// Age after expiry before transitional records are considered stuck.
    pub stale_cutoff: Duration,
    /// Records younger than this are skipped by the healer, so that a sandbox
    /// still being written by its creator is not "repaired" mid-flight.
    pub heal_grace: Duration,
    pub heal_interval: Duration,
    pub reap_interval: Duration,
    /// Read every round as a runtime kill switch.
    pub expiry_healer_enabled: bool,
    pub transition_reaper_enabled: bool,

    pub reserve_result_ttl: Duration,
    /// Maximum age of an unfinished creation reservation.
    pub reserve_stale_ttl: Duration,

    /// Fallback poll interval for every wait in this module, so that a
    /// dropped pub/sub notification costs latency rather than a hang.
    pub poll_interval: Duration,
    /// Chunk size for `MGET`-style batched reads.
    pub batch_chunk: usize,
    /// How long the aggregate memo used by `list_with_callback` stays fresh.
    pub metrics_memo_ttl: Duration,
    /// Whether `update_if_state` takes the distributed lock.
    /// Contention-management switch; CAS predicates still carry correctness.
    pub distributed_lock_enabled: bool,

    /// Per-command response timeout.
    pub response_timeout: Duration,
    /// Fresh connection-attempt timeout.
    pub connect_timeout: Duration,

    /// Test-only switch for mutation probes.
    #[cfg(test)]
    #[serde(skip)]
    pub cas_predicates_enabled: bool,
}

impl Default for RedisStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            lock_ttl: Duration::from_secs(15),
            lock_wait: Duration::from_secs(10),
            lock_retry_min: Duration::from_millis(200),
            lock_retry_max: Duration::from_secs(1),
            lock_retry_jitter: 0.25,
            closure_budget: Duration::from_millis(50),
            write_budget: Duration::from_secs(2),
            record_ttl_grace: Duration::from_secs(3600),
            transition_key_ttl: Duration::from_secs(90),
            transition_result_ttl: Duration::from_secs(30),
            wait_transition_timeout: Duration::from_secs(60),
            max_transition_retries: 3,
            expired_batch_limit: 256,
            stale_cutoff: Duration::from_secs(180),
            heal_grace: Duration::from_secs(60),
            heal_interval: Duration::from_secs(300),
            reap_interval: Duration::from_secs(30),
            expiry_healer_enabled: true,
            transition_reaper_enabled: true,
            reserve_result_ttl: Duration::from_secs(30),
            reserve_stale_ttl: Duration::from_secs(300),
            poll_interval: Duration::from_secs(1),
            batch_chunk: 256,
            metrics_memo_ttl: Duration::from_secs(1),
            distributed_lock_enabled: true,
            response_timeout: Duration::from_millis(1500),
            connect_timeout: Duration::from_millis(2500),
            #[cfg(test)]
            cas_predicates_enabled: true,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("invalid redis store configuration: {0}")]
pub struct RedisStoreConfigError(String);

impl RedisStoreConfig {
    /// Whether test scripts retain their CAS predicates.
    #[cfg(test)]
    pub fn cas_predicates_enabled(&self) -> bool {
        self.cas_predicates_enabled
    }

    /// Validates `transition_key_ttl > wait_transition_timeout > lock_ttl`
    /// and related safety bounds.
    pub fn validate(&self) -> std::result::Result<(), RedisStoreConfigError> {
        let invalid = |msg: String| Err(RedisStoreConfigError(msg));

        if self.transition_key_ttl <= self.wait_transition_timeout {
            return invalid(format!(
                "transition_key_ttl ({:?}) must be greater than wait_transition_timeout ({:?}): \
                 otherwise a waiter is still waiting when the transition key expires, and a \
                 second replica starts a second transition on the same sandbox",
                self.transition_key_ttl, self.wait_transition_timeout
            ));
        }
        if self.wait_transition_timeout <= self.lock_ttl {
            return invalid(format!(
                "wait_transition_timeout ({:?}) must be greater than lock_ttl ({:?})",
                self.wait_transition_timeout, self.lock_ttl
            ));
        }
        if self.stale_cutoff <= self.transition_key_ttl {
            return invalid(format!(
                "stale_cutoff ({:?}) must be greater than transition_key_ttl ({:?}): otherwise a \
                 sandbox that is merely mid-pause is swept as a stuck transition",
                self.stale_cutoff, self.transition_key_ttl
            ));
        }
        if self.write_budget >= self.lock_ttl {
            return invalid(format!(
                "write_budget ({:?}) must be smaller than lock_ttl ({:?}), or no write ever \
                 believes it has time to run",
                self.write_budget, self.lock_ttl
            ));
        }
        if self.record_ttl_grace <= self.transition_key_ttl {
            return invalid(format!(
                "record_ttl_grace ({:?}) must be greater than transition_key_ttl ({:?}): a record \
                 that expires while its VM is still running turns that VM into an orphan, and an \
                 orphan gets killed",
                self.record_ttl_grace, self.transition_key_ttl
            ));
        }
        if self.expired_batch_limit == 0 || self.batch_chunk == 0 {
            return invalid("expired_batch_limit and batch_chunk must be non-zero".to_string());
        }
        if !(0.0..1.0).contains(&self.lock_retry_jitter) {
            return invalid(format!(
                "lock_retry_jitter ({}) must be in [0, 1)",
                self.lock_retry_jitter
            ));
        }
        if self.key_prefix.is_empty() || self.key_prefix.contains("scheduler") {
            return invalid(format!(
                "key_prefix ({:?}) must be non-empty and must not overlap the routing \
                 projection's `agentenv:scheduler:bindings:*` namespace",
                self.key_prefix
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_satisfy_the_ordering_invariant() {
        RedisStoreConfig::default().validate().unwrap();
    }

    #[test]
    fn transition_key_shorter_than_the_wait_is_rejected() {
        let config = RedisStoreConfig {
            transition_key_ttl: Duration::from_secs(30),
            ..Default::default()
        };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("transition_key_ttl"), "{err}");
    }

    #[test]
    fn lock_longer_than_the_wait_is_rejected() {
        let config = RedisStoreConfig {
            lock_ttl: Duration::from_secs(70),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn stale_cutoff_inside_a_legal_transition_is_rejected() {
        let config = RedisStoreConfig {
            stale_cutoff: Duration::from_secs(45),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn record_grace_shorter_than_a_transition_is_rejected() {
        let config = RedisStoreConfig {
            record_ttl_grace: Duration::from_secs(30),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_prefix_that_collides_with_the_routing_projection_is_rejected() {
        let config = RedisStoreConfig {
            key_prefix: "agentenv:scheduler:bindings".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
