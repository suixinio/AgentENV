//! Core Redis implementation of the metadata-store contract.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::super::{
    state_from_token, state_token, FencedRemoval, MetadataRows, MetadataStore,
    MetadataUpdateResult, Result, SandboxListFilter, SandboxMetadata, StoreError,
    TransitionOutcome, TransitionRequest, TransitionSettlement,
};
use super::keys::{routing, ExpiryMember};
use super::record::{to_unix_millis, StoredSandboxRecord};
use super::{backend, backend_msg, scripts, RedisMetadataStore, StoreInner};
use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

/// Record-key TTL update policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlMode {
    /// Preserve the existing TTL for state-only transition stamps.
    Keep,
    /// Recompute TTL from the record's remaining running-time budget.
    Recompute,
}

impl TtlMode {
    /// Resolves the script's mandatory TTL argument.
    pub fn resolve(self, record: &StoredSandboxRecord, grace: Duration) -> String {
        match self {
            TtlMode::Keep => "keep".to_string(),
            TtlMode::Recompute => match record.record_ttl(SystemTime::now(), grace) {
                Some(ttl) => ttl.as_millis().to_string(),
                None => "keep".to_string(),
            },
        }
    }
}

/// What the expiry index has to be told about a write.
struct Rescore {
    needed: bool,
    old_member: String,
    new_member: String,
    new_score: Option<i64>,
}

impl Rescore {
    // Rescore whenever expiry or incarnation changes.
    fn between(previous: &SandboxMetadata, current: &SandboxMetadata) -> Self {
        let needed = previous.expires_at != current.expires_at
            || previous.execution_id != current.execution_id;
        Self {
            needed,
            // Remove any healer-created member for the prior incarnation.
            old_member: ExpiryMember::new(previous.id, previous.execution_id).encode(),
            new_member: ExpiryMember::new(current.id, current.execution_id).encode(),
            new_score: current.expires_at.map(to_unix_millis),
        }
    }
}

impl StoreInner {
    /// Writes one record under revision and optional execution predicates.
    pub async fn write_record(
        &self,
        previous: &SandboxMetadata,
        record: &StoredSandboxRecord,
        expected_rev: u64,
        expected_execution: Option<ExecutionId>,
        ttl: TtlMode,
    ) -> Result<()> {
        let sandbox_id = record.sandbox_id();
        let rescore = Rescore::between(previous, &record.metadata);

        let ttl_argument = ttl.resolve(record, self.config().record_ttl_grace);

        let mut connection = self.connection();
        let outcome: i64 = self
            .update_script()
            .key(self.keys().record(&sandbox_id))
            .key(self.keys().expiry())
            .arg(record.encode()?)
            .arg(expected_rev.to_string())
            .arg(
                expected_execution
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            )
            .arg(ttl_argument)
            .arg(if rescore.needed { "1" } else { "0" })
            .arg(if rescore.needed {
                rescore.old_member.clone()
            } else {
                String::new()
            })
            .arg(match (rescore.needed, rescore.new_score) {
                (true, Some(_)) => rescore.new_member.clone(),
                _ => String::new(),
            })
            .arg(rescore.new_score.unwrap_or_default().to_string())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        match outcome {
            scripts::UPDATE_OK => {
                self.invalidate_listing_memo().await;
                self.notify(&routing::record(&sandbox_id)).await;
                Ok(())
            }
            scripts::UPDATE_NOT_FOUND => Err(StoreError::SandboxNotFound { sandbox_id }),
            scripts::UPDATE_UNDECODABLE => Err(backend_msg(format!(
                "sandbox {sandbox_id} record could not be decoded by the update script"
            ))),
            scripts::UPDATE_REV_MISMATCH => Err(StoreError::ConcurrentUpdate { sandbox_id }),
            scripts::UPDATE_EXECUTION_MISMATCH => {
                let actual = self
                    .read_record(&sandbox_id)
                    .await?
                    .map(|r| r.execution_id());
                warn!(
                    %sandbox_id,
                    refusal_code = "execution_superseded",
                    expected = ?expected_execution,
                    ?actual,
                    "refused a write for a superseded incarnation"
                );
                Err(StoreError::ExecutionSuperseded {
                    sandbox_id,
                    expected: expected_execution.unwrap_or(previous.execution_id),
                    actual,
                })
            }
            other => Err(backend_msg(format!(
                "update script returned an unexpected code {other} for sandbox {sandbox_id}"
            ))),
        }
    }

    /// Compare-and-set state with safe optimistic retries.
    pub async fn compare_and_set_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> Result<SandboxState> {
        for _ in 0..3 {
            let current = self.require_record(sandbox_id).await?;
            let actual_state = current.metadata.state;
            if !expected_states.contains(&actual_state) {
                return Err(StoreError::StateConflict {
                    sandbox_id: *sandbox_id,
                    expected_states: expected_states.to_vec(),
                    actual_state,
                });
            }

            let mut metadata = current.metadata.clone();
            metadata.state = new_state;
            metadata.sync_running_clock(SystemTime::now());

            let mut next = StoredSandboxRecord::new(&metadata, current.rev.saturating_add(1))?;
            next.inherit_placement_from(&current);

            match self
                .write_record(
                    &current.metadata,
                    &next,
                    current.rev,
                    Some(current.execution_id()),
                    TtlMode::Recompute,
                )
                .await
            {
                Ok(()) => return Ok(actual_state),
                Err(StoreError::ConcurrentUpdate { .. }) => continue,
                Err(StoreError::ExecutionSuperseded { .. }) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(StoreError::ConcurrentUpdate {
            sandbox_id: *sandbox_id,
        })
    }

    /// Deletes a record and all index entries.
    pub async fn remove_record(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        let mut connection = self.connection();
        let removed: Option<Vec<u8>> = scripts::remove()
            .key(self.keys().record(sandbox_id))
            .key(self.keys().index())
            .key(self.keys().expiry())
            .arg(sandbox_id.to_string())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        self.invalidate_listing_memo().await;
        self.notify(&routing::record(sandbox_id)).await;

        removed
            .as_deref()
            .map(StoredSandboxRecord::decode)
            .transpose()
            .map(|record| record.map(StoredSandboxRecord::into_metadata))
    }

    /// Reads parseable ids from the membership set.
    async fn member_ids(&self) -> Result<Vec<SandboxId>> {
        let mut connection = self.connection();
        let raw: Vec<String> = redis::cmd("SMEMBERS")
            .arg(self.keys().index())
            .query_async(&mut connection)
            .await
            .map_err(backend)?;

        let mut ids = Vec::with_capacity(raw.len());
        for member in raw {
            match SandboxId::parse_str(&member) {
                Ok(id) => ids.push(id),
                Err(_) => {
                    // Invalid members are garbage, not absent sandboxes.
                    warn!(member = %member, "dropping an unparseable membership entry");
                    let _: redis::RedisResult<i64> = redis::cmd("SREM")
                        .arg(self.keys().index())
                        .arg(&member)
                        .query_async(&mut connection)
                        .await;
                }
            }
        }
        Ok(ids)
    }

    /// Reads all member records, failing the whole call if any chunk errors.
    async fn read_all_records(&self) -> Result<Vec<SandboxMetadata>> {
        let ids = self.member_ids().await?;
        let mut records = Vec::with_capacity(ids.len());
        let mut connection = self.connection();

        for chunk in ids.chunks(self.config().batch_chunk) {
            let keys: Vec<String> = chunk.iter().map(|id| self.keys().record(id)).collect();
            let raws: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;

            for (id, raw) in chunk.iter().zip(raws) {
                match raw {
                    Some(raw) => records.push(StoredSandboxRecord::decode(&raw)?.into_metadata()),
                    // Expired record keys leave stale membership entries.
                    None => self.sweep_index_member(id).await?,
                }
            }
        }
        Ok(records)
    }

    /// Atomically drops a membership entry only when its record is absent.
    pub async fn sweep_index_member(&self, sandbox_id: &SandboxId) -> Result<()> {
        let mut connection = self.connection();
        let removed: i64 = scripts::sweep_index_member()
            .key(self.keys().index())
            .key(self.keys().record(sandbox_id))
            .arg(sandbox_id.to_string())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        if removed == 1 {
            debug!(%sandbox_id, "swept a membership entry whose record had expired");
        }
        Ok(())
    }
}

/// Waits for a notification or poll interval; closed channels fall back to sleep.
pub async fn wake_or_poll(wake: &mut broadcast::Receiver<()>, poll: Duration) {
    tokio::select! {
        received = wake.recv() => {
            if matches!(received, Err(broadcast::error::RecvError::Closed)) {
                tokio::time::sleep(poll).await;
            }
        }
        _ = tokio::time::sleep(poll) => {}
    }
}

#[async_trait]
impl MetadataStore for RedisMetadataStore {
    async fn add(&self, mut metadata: SandboxMetadata) -> Result<()> {
        let inner = self.inner();
        let now = SystemTime::now();
        metadata.sync_running_clock(now);

        let sandbox_id = metadata.id;
        let record = StoredSandboxRecord::new(&metadata, 1)?;
        let ttl = record
            .record_ttl(now, inner.config().record_ttl_grace)
            .map(|ttl| ttl.as_millis().to_string())
            .unwrap_or_default();
        let expiry_member = ExpiryMember::new(sandbox_id, metadata.execution_id).encode();

        let mut connection = inner.connection();
        let created: i64 = scripts::add()
            .key(inner.keys().record(&sandbox_id))
            .key(inner.keys().index())
            .key(inner.keys().expiry())
            .arg(record.encode()?)
            .arg(ttl)
            .arg(
                metadata
                    .expires_at
                    .map(|at| to_unix_millis(at).to_string())
                    .unwrap_or_default(),
            )
            .arg(expiry_member)
            .arg(sandbox_id.to_string())
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        if created == 0 {
            return Err(StoreError::SandboxAlreadyExists { sandbox_id });
        }

        inner.invalidate_listing_memo().await;
        inner.notify(&routing::record(&sandbox_id)).await;
        Ok(())
    }

    /// Full-record update fenced by the currently stored execution and revision.
    async fn update(&self, mut metadata: SandboxMetadata) -> Result<()> {
        let inner = self.inner();
        let sandbox_id = metadata.id;
        metadata.sync_running_clock(SystemTime::now());

        for _ in 0..3 {
            let current = inner.require_record(&sandbox_id).await?;
            if current.execution_id() != metadata.execution_id {
                warn!(
                    %sandbox_id,
                    refusal_code = "execution_superseded",
                    expected = %metadata.execution_id,
                    actual = %current.execution_id(),
                    "refused a full-record write for a superseded incarnation"
                );
                return Err(StoreError::ExecutionSuperseded {
                    sandbox_id,
                    expected: metadata.execution_id,
                    actual: Some(current.execution_id()),
                });
            }

            let mut next = StoredSandboxRecord::new(&metadata, current.rev.saturating_add(1))?;
            next.inherit_placement_from(&current);

            match inner
                .write_record(
                    &current.metadata,
                    &next,
                    current.rev,
                    Some(metadata.execution_id),
                    TtlMode::Recompute,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(StoreError::ConcurrentUpdate { .. }) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(StoreError::ConcurrentUpdate { sandbox_id })
    }

    /// State CAS is already atomic and does not take the distributed lock.
    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> Result<SandboxState> {
        self.inner()
            .compare_and_set_state(sandbox_id, new_state, expected_states)
            .await
    }

    /// Executes the callback once on a local copy, then writes under lock-lifetime,
    /// revision, and execution predicates.
    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> Result<MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        let inner = self.inner();
        let config = inner.config();

        let mut wake = inner.subscribe(&routing::lock(sandbox_id));
        let lock = if config.distributed_lock_enabled {
            Some(
                inner
                    .locks()
                    .acquire(sandbox_id, inner.keys().lock(sandbox_id), &mut wake)
                    .await?,
            )
        } else {
            None
        };

        let outcome = self
            .update_if_state_locked(sandbox_id, expected_states, update, lock.as_ref())
            .await;

        // Release and notify on both success and failure.
        if let Some(lock) = lock {
            if let Err(error) = inner.locks().release(lock).await {
                debug!(%sandbox_id, %error, "failed to release the sandbox lock; it will expire");
            }
            inner.notify(&routing::lock(sandbox_id)).await;
        }

        outcome
    }

    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self
            .inner()
            .read_record(sandbox_id)
            .await?
            .map(StoredSandboxRecord::into_metadata))
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        self.inner().remove_record(sandbox_id).await
    }

    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: ExecutionId,
        expected_states: &[SandboxState],
    ) -> Result<FencedRemoval> {
        let inner = self.inner();
        let mut connection = inner.connection();
        let mut invocation = scripts::remove_if_execution().prepare_invoke();
        invocation
            .key(inner.keys().record(sandbox_id))
            .key(inner.keys().index())
            .key(inner.keys().expiry())
            .arg(sandbox_id.to_string())
            .arg(expected_execution_id.to_string());
        // Variadic states begin after fixed script arguments.
        for state in expected_states {
            invocation.arg(state_token(*state));
        }

        let (code, state, execution): (i64, String, String) = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;

        match code {
            scripts::FENCED_REMOVE_REMOVED => {
                inner.invalidate_listing_memo().await;
                inner.notify(&routing::record(sandbox_id)).await;
                Ok(FencedRemoval::Removed)
            }
            scripts::FENCED_REMOVE_ABSENT => Ok(FencedRemoval::Absent),
            scripts::FENCED_REMOVE_SUPERSEDED => {
                // Refuse unreadable superseding details rather than guessing ownership.
                let (Some(state), Ok(execution_id)) =
                    (state_from_token(&state), ExecutionId::parse_str(&execution))
                else {
                    return Err(backend_msg(format!(
                        "sandbox {sandbox_id} record refused a fenced removal with an \
                         unreadable state {state:?} or incarnation {execution:?}"
                    )));
                };
                Ok(FencedRemoval::Superseded {
                    state,
                    execution_id,
                })
            }
            scripts::FENCED_REMOVE_UNDECODABLE => Err(backend_msg(format!(
                "sandbox {sandbox_id} record could not be decoded by the fenced removal script"
            ))),
            other => Err(backend_msg(format!(
                "fenced removal script returned an unknown code {other} for sandbox {sandbox_id}"
            ))),
        }
    }

    async fn list(&self) -> Result<Vec<SandboxMetadata>> {
        self.inner().read_all_records().await
    }

    /// Visits a memoized aggregate sample; targeted listings remain live reads.
    async fn list_with_callback<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        let inner = self.inner();
        let records = match inner.memoised_listing().await {
            Some(records) => records,
            None => {
                let fresh = inner.read_all_records().await?;
                inner.store_listing_memo(fresh).await
            }
        };
        for record in records.iter() {
            callback(record);
        }
        Ok(())
    }

    /// Applies filters after an authoritative full read.
    async fn list_filtered(&self, filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>> {
        let records = self.inner().read_all_records().await?;
        Ok(records
            .into_iter()
            .filter(|metadata| {
                let state_matches = filter
                    .states
                    .as_ref()
                    .is_none_or(|states| states.contains(&metadata.state));
                let excluded = filter
                    .excluded_states
                    .as_ref()
                    .is_some_and(|states| states.contains(&metadata.state));
                state_matches
                    && !excluded
                    && crate::orchestrator::store::user_metadata_matches(
                        metadata.user_metadata.as_ref(),
                        filter.user_metadata.as_ref(),
                    )
            })
            .collect())
    }

    async fn list_expired(&self, now: SystemTime) -> Result<Vec<SandboxMetadata>> {
        self.expired_batch(now, usize::MAX).await
    }

    /// Lists ids from the maintained membership set, never a best-effort key scan.
    async fn list_ids(&self) -> Result<Vec<SandboxId>> {
        let inner = self.inner();
        let ids = inner.member_ids().await?;
        let mut connection = inner.connection();
        let mut alive = Vec::with_capacity(ids.len());

        for chunk in ids.chunks(inner.config().batch_chunk) {
            let mut pipeline = redis::pipe();
            for id in chunk {
                pipeline.cmd("EXISTS").arg(inner.keys().record(id));
            }
            let present: Vec<i64> = pipeline
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            for (id, exists) in chunk.iter().zip(present) {
                if exists == 1 {
                    alive.push(*id);
                } else {
                    inner.sweep_index_member(id).await?;
                }
            }
        }
        Ok(alive)
    }

    /// Waits for state settlement; only record absence returns `None`.
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> Result<Option<SandboxMetadata>> {
        let inner = self.inner();
        // Subscribe before reading to avoid a lost-wake window.
        let mut wake = inner.subscribe(&routing::record(sandbox_id));

        loop {
            match inner.read_record(sandbox_id).await? {
                None => return Ok(None),
                Some(record) if !transitional_states.contains(&record.metadata.state) => {
                    return Ok(Some(record.into_metadata()));
                }
                Some(_) => {}
            }
            wake_or_poll(&mut wake, inner.config().poll_interval).await;
        }
    }

    async fn expired_batch(&self, now: SystemTime, limit: usize) -> Result<Vec<SandboxMetadata>> {
        super::expiry::expired_batch(self.inner(), now, limit).await
    }

    /// Reads in chunks and reports coverage only after every chunk succeeds.
    async fn get_many(&self, ids: &[SandboxId]) -> Result<MetadataRows> {
        if ids.is_empty() {
            return Ok(MetadataRows::default());
        }
        let inner = self.inner();
        let mut connection = inner.connection();
        let mut entries = HashMap::with_capacity(ids.len());

        for chunk in ids.chunks(inner.config().batch_chunk) {
            let keys: Vec<String> = chunk.iter().map(|id| inner.keys().record(id)).collect();
            let raws: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            for (id, raw) in chunk.iter().zip(raws) {
                if let Some(raw) = raw {
                    entries.insert(*id, StoredSandboxRecord::decode(&raw)?.into_metadata());
                }
            }
        }

        Ok(MetadataRows {
            entries,
            covered: ids.to_vec(),
        })
    }

    async fn start_transition(
        &self,
        sandbox_id: &SandboxId,
        request: TransitionRequest,
    ) -> Result<TransitionOutcome> {
        super::transition::start_transition(self.inner(), sandbox_id, request).await
    }

    async fn transition_settlement(
        &self,
        sandbox_id: &SandboxId,
        transition_id: &str,
    ) -> Result<TransitionSettlement> {
        let transition_id = uuid::Uuid::parse_str(transition_id).map_err(|source| {
            backend_msg(format!(
                "{transition_id:?} is not a transition id: {source}"
            ))
        })?;
        super::transition::read_transition_result(self.inner(), sandbox_id, &transition_id).await
    }

    async fn heal_expiry_index(&self) -> Result<usize> {
        super::expiry::heal_expiry_index(self.inner()).await
    }

    async fn reap_stuck_transitions(&self, now: SystemTime) -> Result<Vec<SandboxId>> {
        super::transition::reap_stuck_transitions(self.inner(), now).await
    }
}

impl RedisMetadataStore {
    async fn update_if_state_locked<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
        lock: Option<&super::lock::SandboxLock>,
    ) -> Result<MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        let inner = self.inner();
        let config = inner.config();

        let current = inner.require_record(sandbox_id).await?;
        let actual_state = current.metadata.state;
        if !expected_states.contains(&actual_state) {
            return Err(StoreError::StateConflict {
                sandbox_id: *sandbox_id,
                expected_states: expected_states.to_vec(),
                actual_state,
            });
        }

        let previous = current.metadata.clone();
        let mut mutated = current.metadata.clone();

        let started = Instant::now();
        update(&mut mutated);
        let elapsed = started.elapsed();
        mutated.sync_running_clock(SystemTime::now());

        // Discard over-budget callback results before touching Redis.
        if elapsed > config.closure_budget {
            metrics::counter!("agentenv_store_closure_budget_exceeded_total").increment(1);
            warn!(
                %sandbox_id,
                ?elapsed,
                budget = ?config.closure_budget,
                "discarded a metadata update whose callback exceeded its budget; \
                 a callback that slow is doing blocking work it should not be doing"
            );
            return Err(StoreError::ClosureBudgetExceeded {
                sandbox_id: *sandbox_id,
                elapsed,
            });
        }

        // Require enough remaining lock lifetime for the write.
        if let Some(lock) = lock {
            if lock.remaining() <= config.write_budget {
                metrics::counter!("agentenv_store_lock_lapsed_total").increment(1);
                warn!(
                    %sandbox_id,
                    held_for = ?lock.held_for(),
                    "abandoned a metadata update whose lock was about to lapse"
                );
                return Err(StoreError::LockLapsed {
                    sandbox_id: *sandbox_id,
                    held_for: lock.held_for(),
                });
            }
        }

        let mut next = StoredSandboxRecord::new(&mutated, current.rev.saturating_add(1))?;
        next.inherit_placement_from(&current);

        // Final revision and execution predicates reject superseding writes.
        inner
            .write_record(
                &previous,
                &next,
                current.rev,
                Some(previous.execution_id),
                TtlMode::Recompute,
            )
            .await?;

        Ok(MetadataUpdateResult {
            previous,
            current: next.into_metadata(),
        })
    }
}
