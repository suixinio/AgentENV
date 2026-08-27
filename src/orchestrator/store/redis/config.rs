use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The default key prefix. 🔴 Disjoint from `agentenv:scheduler:bindings:*`,
/// which is the routing projection and is owned by a different subsystem with
/// a different lifetime. Nothing in this module may read or write a key under
/// that prefix.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:api";

/// Tuning for [`RedisMetadataStore`][super::RedisMetadataStore].
///
/// Serde-ready so an assembly point can flatten it under `[orchestrator.store]`,
/// but deliberately not wired into `cfg.rs` here: nothing constructs the store
/// in this batch, and a TOML block that no loader parses is a setting that
/// looks configurable and is not.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisStoreConfig {
    /// `redis://host:port[/db]`.
    pub url: String,
    /// Prefix for every key this store owns.
    pub key_prefix: String,

    // -- primitive one: closure update --------------------------------------
    /// How long the per-sandbox lock lives.
    ///
    /// 🔴 15s, a quarter of e2b's 60s, and on purpose. e2b's lock has to cover
    /// a long chain of round trips; ours covers one `GET`, one pure callback
    /// and one `EVAL`, which is under 5ms in the normal case. The number is
    /// really an answer to "how long may a killed replica block one sandbox".
    pub lock_ttl: Duration,
    /// How long to wait to acquire the lock before giving up.
    ///
    /// 🔴 Separate from `lock_ttl`. e2b uses one value for both, which means
    /// shortening the TTL necessarily shortens the wait; the two answer
    /// different questions and holding them hostage to each other means
    /// neither can be tuned.
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

    // -- record lifetime ----------------------------------------------------
    /// Added to a record's remaining lifetime budget to give the record key a
    /// TTL.
    ///
    /// 🔴 Must be comfortably larger than the longest single transition. A
    /// record that disappears while its VM is still running turns that VM into
    /// an orphan, and the treatment for an orphan is to kill it. This is a
    /// leak backstop, not a deadline.
    pub record_ttl_grace: Duration,

    // -- primitive two: transitions -----------------------------------------
    pub transition_key_ttl: Duration,
    pub transition_result_ttl: Duration,
    /// The caller-side patience that `wait_while_in_states` is wrapped in.
    ///
    /// 🔴 Not used by the store to time anything out. It is here so that
    /// [`RedisStoreConfig::validate`] can enforce the ordering below; the
    /// assembly point must pass the orchestrator's real value.
    pub wait_transition_timeout: Duration,
    /// How many times `start_transition` may wait for an in-flight transition
    /// and try again.
    ///
    /// 🔴 A bound, where e2b recurses without one. On a sandbox that is being
    /// transitioned repeatedly, an unbounded retry can eat a whole request
    /// budget with nothing on the stack to say how deep it went.
    pub max_transition_retries: u32,

    // -- primitive three: expiry --------------------------------------------
    pub expired_batch_limit: usize,
    /// How long past its expiry a non-`Running` record must sit before it is
    /// treated as a stuck transition rather than a normal in-flight one.
    ///
    /// 🔴 Must be strictly greater than the longest legal transition, or a
    /// sandbox that is merely pausing gets swept as stuck.
    pub stale_cutoff: Duration,
    /// Records younger than this are skipped by the healer, so that a sandbox
    /// still being written by its creator is not "repaired" mid-flight.
    pub heal_grace: Duration,
    pub heal_interval: Duration,
    pub reap_interval: Duration,
    /// 🔴 Read afresh every round, so it acts as a kill switch without a
    /// redeploy.
    pub expiry_healer_enabled: bool,
    pub transition_reaper_enabled: bool,

    // -- primitive four: reservations ---------------------------------------
    pub reserve_result_ttl: Duration,
    /// How long a creation window may stay open before another caller may take
    /// the id.
    ///
    /// 🔴 Must exceed the slowest cold start there is. e2b uses 90s and calls
    /// it "well beyond any realistic sandbox creation time"; ours can pull OCI
    /// layers and convert them to overlaybd first, so 90s would mean handing a
    /// half-built sandbox's id to somebody else.
    pub reserve_stale_ttl: Duration,

    // -- shared -------------------------------------------------------------
    /// Fallback poll interval for every wait in this module, so that a
    /// dropped pub/sub notification costs latency rather than a hang.
    pub poll_interval: Duration,
    /// Chunk size for `MGET`-style batched reads.
    pub batch_chunk: usize,
    /// How long the aggregate memo used by `list_with_callback` stays fresh.
    pub metrics_memo_ttl: Duration,
    /// Whether `update_if_state` takes the distributed lock.
    ///
    /// 🔴 A throughput and "the callback runs once" switch, not a correctness
    /// switch. Turning it off must not corrupt anything — it makes contended
    /// updates fail with `ConcurrentUpdate` instead of queueing. That is
    /// precisely what makes the probe in the design doc able to tell the lock
    /// and the CAS predicates apart.
    pub distributed_lock_enabled: bool,

    // -- connection timeouts -------------------------------------------------
    /// How long a single Redis command may run before
    /// `redis::aio::ConnectionManager` times it out and reconnects.
    /// redis-rs 1.6.0's own built-in default (applied by a bare
    /// `ConnectionManager::new(client)`, which is what `RedisMetadataStore::connect`
    /// constructed until this field existed) is 500ms — measured too tight
    /// against this exact store's own production traffic on the pve-sg
    /// cluster: a `ZRANGEBYSCORE` on `super::expiry`'s expiry index was
    /// observed taking 292ms, and cross-node RTT stacks on top of that
    /// server-side latency, on every command, not only this one. 3x the
    /// crate default: bounded and short, but with real headroom over an
    /// already-observed worst case.
    ///
    /// 🔴 The *default* (1.5s) is deliberately kept below `write_budget`'s
    /// own default (2s), not equal to it: `write_budget` is "headroom the
    /// write itself is assumed to need," checked against the lock's
    /// remaining lifetime before a write is attempted (see that field's
    /// own doc and `crud.rs`'s `lock.remaining() <= config.write_budget`
    /// check) — a `response_timeout` at or above `write_budget` would let a
    /// single slow command alone eat the entire budget that check exists
    /// to reserve. This is a default-vs-default relationship only, not a
    /// `validate`-enforced invariant: several tests in this module
    /// deliberately shrink `write_budget` far below any sane connection
    /// timeout (down to 100ms) to exercise the lock-lapse path in
    /// `crud.rs` in well under a second, and a real deployment has no
    /// comparable reason to shrink `write_budget` — see this field's own
    /// production-latency justification above, which does not shrink with
    /// it.
    ///
    /// Trade-off: a genuinely unreachable Redis now takes up to this long
    /// (per call, not cumulative) to be detected, instead of 500ms. Audited
    /// as acceptable: nothing in `src/orchestrator/` races this store's
    /// calls against a shorter deadline in a `select!` (the only `select!`
    /// in `src/orchestrator/service.rs`'s eviction/shutdown loops is
    /// against `shutdown_rx`/a ticker, never against a store call), and no
    /// health/readiness probe touches this store synchronously
    /// (`src/api/server.rs`'s `/health` is a bare `"ok"`). Every caller of
    /// this store already treats a backend error as retryable — the write
    /// path returns `StoreError::Backend` up to the orchestrator, which
    /// logs and the caller (an HTTP request, or the auto-evict/healer/reaper
    /// background loops) retries on its own next round either way.
    pub response_timeout: Duration,
    /// How long a fresh TCP connection attempt (initial connect, or a
    /// reconnect after a `response_timeout`) may take. Bounds only
    /// connection setup, not command execution.
    pub connect_timeout: Duration,

    /// 🔴 Test-only, and there is no field for it in a non-test build.
    ///
    /// It removes the `rev`/`execution_id` predicates from the write scripts,
    /// which is the only way to demonstrate that they are what carries
    /// correctness. A runtime switch able to do that in production would be an
    /// instance of exactly the class of thing this work exists to remove.
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
    /// Whether the write scripts carry their `rev`/`execution_id` predicates.
    ///
    /// 🔴 There is no non-test counterpart, and that is the point: outside a
    /// test build there is no field, no accessor and no branch — the predicates
    /// cannot be turned off by anything.
    #[cfg(test)]
    pub fn cas_predicates_enabled(&self) -> bool {
        self.cas_predicates_enabled
    }

    /// 🔴 The ordering `transition_key_ttl > wait_transition_timeout > lock_ttl`
    /// is an invariant, not a preference.
    ///
    /// Invert the first inequality and this happens: a caller is still inside
    /// its 60-second wait when the transition key expires, another replica sees
    /// no transition in flight and starts a second one, and the same sandbox is
    /// paused twice by two machines that both believe they are the only one.
    /// Invert the second and a lock outlives the operation that took it.
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

    /// The control for the test above: the invariant has to be able to fail.
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
