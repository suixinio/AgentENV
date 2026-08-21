//! The twelve original [`MetadataStore`] methods, on Redis.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::super::{
    MetadataRows, MetadataStore, MetadataUpdateResult, Reservation, Result, SandboxListFilter,
    SandboxMetadata, StoreError, TransitionOutcome, TransitionRequest, TransitionSettlement,
};
use super::keys::{routing, ExpiryMember};
use super::record::{to_unix_millis, StoredSandboxRecord};
use super::{backend, backend_msg, scripts, RedisMetadataStore, StoreInner};
use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

/// How a write decides what happens to the record key's TTL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TtlMode {
    /// Leave whatever TTL the key already has.
    ///
    /// Used where a write deliberately must not touch the lifetime budget —
    /// the mid-operation state stamp in `start_transition` is the only one.
    Keep,
    /// Derive the TTL again from the record being written.
    ///
    /// 🔴 A deliberate departure from e2b, which uses `KEEPTTL` everywhere.
    /// e2b can: its sandbox keys have no TTL at all, so its `KEEPTTL` preserves
    /// nothing and is pure defence against a stray `SET`. Ours is a pure
    /// function of the record — `lifetime_deadline` plus a grace — so deriving
    /// it again is idempotent for a running sandbox and *necessary* for a
    /// paused one: paused time is free, so a paused sandbox's deadline recedes
    /// with the clock, and a TTL frozen at creation would delete the record of
    /// a sandbox that is still perfectly alive on somebody's disk.
    ///
    /// The failure `KEEPTTL` guards against is still guarded against: the mode
    /// is a required argument of the script, so no write path can reach a bare
    /// `SET` by omission.
    Recompute,
}

impl TtlMode {
    /// The literal the scripts take. 🔴 Every write names one; there is no
    /// default, because both wrong answers are silent.
    pub(super) fn resolve(self, record: &StoredSandboxRecord, grace: Duration) -> String {
        match self {
            TtlMode::Keep => "keep".to_string(),
            TtlMode::Recompute => match record.record_ttl(SystemTime::now(), grace) {
                Some(ttl) => ttl.as_millis().to_string(),
                // No configured ceiling means no leak bound to enforce, and an
                // unbounded key, exactly as e2b has.
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
    /// 🔴 The incarnation is part of the trigger, not just the expiry.
    ///
    /// e2b only compares end times, because its `Update` never changes an
    /// incarnation — a new incarnation there arrives through `Add`. Ours does:
    /// the finishing write of a resume installs a new one through this very
    /// path. Compare only the expiry and that write leaves the previous
    /// incarnation's member in the index and never inserts the live one, so the
    /// sandbox silently stops being able to expire.
    fn between(previous: &SandboxMetadata, current: &SandboxMetadata) -> Self {
        let needed = previous.expires_at != current.expires_at
            || previous.execution_id != current.execution_id;
        Self {
            needed,
            // Always removed when rescoring, whether or not the previous record
            // carried an expiry: the healer indexes expiry-less records too, at
            // their lifetime deadline, and that member is stale now as well.
            old_member: ExpiryMember::new(previous.id, previous.execution_id).encode(),
            new_member: ExpiryMember::new(current.id, current.execution_id).encode(),
            new_score: current.expires_at.map(to_unix_millis),
        }
    }
}

impl StoreInner {
    /// The single write path. Every predicate this store has is applied here.
    ///
    /// Returns `Err(ConcurrentUpdate)` when the revision moved and
    /// `Err(ExecutionSuperseded)` when the incarnation did.
    pub(super) async fn write_record(
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

    /// Compare-and-set on state alone.
    ///
    /// An optimistic retry is safe here — unlike in `update_if_state` there is
    /// no caller-supplied callback, so re-running costs nothing and observes
    /// nothing.
    pub(super) async fn compare_and_set_state(
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

    /// Deletes a record and everything that indexes it.
    pub(super) async fn remove_record(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<Option<SandboxMetadata>> {
        let mut connection = self.connection();
        let removed: Option<Vec<u8>> = scripts::remove()
            .key(self.keys().record(sandbox_id))
            .key(self.keys().index())
            .key(self.keys().expiry())
            .key(self.keys().pending())
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

    /// Reads the membership set, dropping members that no longer parse.
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
                    // 🔴 Garbage, not a sandbox. Swept rather than reported, so
                    // that it can never be mistaken for a record that is gone.
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

    /// Reads every record in the membership set.
    ///
    /// 🔴 All or nothing. A chunk that fails aborts the whole call rather than
    /// returning a shorter list, because a shorter list is indistinguishable
    /// from "those sandboxes are gone" — and the caller's answer to *that* is
    /// to tear things down.
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
                    // The record key's TTL is the only thing that removes a
                    // record without going through `remove`, so a member with
                    // no record is a leak backstop having fired.
                    None => self.sweep_index_member(id).await?,
                }
            }
        }
        Ok(records)
    }

    /// Drops a membership entry whose record is gone.
    ///
    /// 🔴 The existence check is inside the script. Checking here and removing
    /// afterwards would let a lockless `add` land in between and have its
    /// brand-new sandbox removed from the membership set.
    pub(super) async fn sweep_index_member(&self, sandbox_id: &SandboxId) -> Result<()> {
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

/// Waits for a wake-up or for the poll interval, whichever comes first.
///
/// 🔴 A closed channel must fall back to sleeping. `recv` on a closed broadcast
/// returns immediately and for ever, so selecting on it without this would turn
/// every wait in the module into a busy loop the moment the notifier stopped.
pub(super) async fn wake_or_poll(wake: &mut broadcast::Receiver<()>, poll: Duration) {
    tokio::select! {
        received = wake.recv() => {
            if matches!(received, Err(broadcast::error::RecvError::Closed)) {
                tokio::time::sleep(poll).await;
            }
        }
        _ = tokio::time::sleep(poll) => {}
    }
}

fn user_metadata_matches(
    metadata: &SandboxMetadata,
    required: Option<&HashMap<String, String>>,
) -> bool {
    required.is_none_or(|required| {
        metadata.user_metadata.as_ref().is_some_and(|actual| {
            required
                .iter()
                .all(|(key, value)| actual.get(key) == Some(value))
        })
    })
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
            .key(inner.keys().pending())
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
        inner.notify(&routing::reservation(&sandbox_id)).await;
        Ok(())
    }

    /// A full, unpredicated overwrite in the trait — but not on the wire.
    ///
    /// 🔴 The incarnation check in step one is new semantics, and it is what
    /// closes the window this method has always had: the pause path reads a
    /// record, writes hundreds of milliseconds' worth of bytes to disk, and
    /// only then writes the record back. In one process a transitional state
    /// kept concurrent writers out of that window. Across replicas it still
    /// keeps out concurrent `update_if_state` calls — but not `add`, which is
    /// lockless, and which `restore_sandbox` performs under a caller-supplied
    /// id. Without this check that `add`'s brand-new incarnation would be
    /// overwritten by a record read before it existed.
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

    /// 🔴 Deliberately does not take the lock.
    ///
    /// Read, compare and write are one script, so the lock would buy only two
    /// extra round trips. And nine of this method's fourteen call sites are
    /// rollback paths: making a rollback queue behind a lock another replica
    /// may hold for fifteen seconds is injecting a failure into the remedy for
    /// a failure.
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

    /// The closure contract, kept word for word, with its scope widened from
    /// this process to the cluster.
    ///
    /// Three layers stop a slow callback from doing damage, and only the third
    /// carries weight:
    ///
    /// 1. The callback mutates a **local copy**, so a callback that overruns
    ///    its budget can be discarded losslessly. It is not retried: `FnOnce`
    ///    forbids it, and `keep_alive`'s callback sets a flag declared outside
    ///    itself that the caller reads afterwards, so a second run would set it
    ///    a second time within one logical operation.
    /// 2. Before writing, the lock's remaining lifetime is checked against the
    ///    write's budget. This catches the things a callback timer cannot: a GC
    ///    pause, scheduler starvation, a stalled Redis.
    /// 3. The write itself carries `rev` and `execution_id` predicates. If both
    ///    layers above leak, the write still cannot overwrite a newer
    ///    incarnation — it fails loudly instead.
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

        // 🔴 Released whether the update succeeded or failed, and the waiters
        // are woken either way: a lock held through a failure is a sandbox
        // nobody else can touch for the rest of its TTL.
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

    async fn list(&self) -> Result<Vec<SandboxMetadata>> {
        self.inner().read_all_records().await
    }

    /// 🔴 The one aggregate path, and the one that is memoised.
    ///
    /// Its only caller is `metrics_snapshot`, which is on the `/metrics` scrape
    /// path and on the node API. In one process it was a read lock and a walk;
    /// on Redis it is a `SMEMBERS` plus an `MGET` of the entire keyspace, and
    /// N replicas times a scrape interval times the whole keyspace is a load
    /// nobody designed.
    ///
    /// So this returns a **sample**, not a point-in-time truth, and
    /// [`RedisMetadataStore::last_listing_sample_at`] says when it was taken.
    /// The other listing methods deliberately do not use the memo: this one is
    /// an aggregate over everything, and a caller asking about *particular*
    /// sandboxes is asking a different question.
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

    /// 🔴 Filtering happens here, not in Redis, and the reason is not only
    /// cost.
    ///
    /// A secondary index per state would put a predicate *inside the store*,
    /// and a predicate baked into a backend answers every question asked of
    /// that backend — including the questions it was not written for. That is
    /// how a catalog whose entire read face was pinned to "ready" came to
    /// return 404 for every template and, more expensively, to treat "the row
    /// is there but not yet ready" as "the row is gone". A read scope belongs
    /// to a *face*, and `list_filtered` has several.
    ///
    /// The cost — one full read per call — is the same one the memo above
    /// records, and is accepted knowingly.
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
                    && user_metadata_matches(metadata, filter.user_metadata.as_ref())
            })
            .collect())
    }

    async fn list_expired(&self, now: SystemTime) -> Result<Vec<SandboxMetadata>> {
        self.expired_batch(now, usize::MAX).await
    }

    /// 🔴 Reads the membership set, never `SCAN`.
    ///
    /// `SCAN` gives a best-effort view of a keyspace that is changing under it;
    /// membership is a fact the write paths maintain atomically. Members whose
    /// record has expired out from under them are dropped here rather than
    /// reported, because a roster entry with no record is what makes a live VM
    /// look like an orphan.
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

    /// Waits for a state, not for a transition id.
    ///
    /// 🔴 Contract unchanged: `Ok(None)` still means only "the record is gone".
    /// In particular a record still in a transitional state whose transition
    /// key has expired is **not** an error here — reporting one would turn a
    /// perfectly ordinary concurrent pause into a failure for the caller that
    /// joined it. Deciding what to do about a transition whose owner died is
    /// the reaper's job, not a waiter's.
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> Result<Option<SandboxMetadata>> {
        let inner = self.inner();
        // 🔴 Subscribe first, then read. The other order has a window in which
        // the state changes after the read and before the subscription.
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

    /// 🔴 Chunked, and every chunk must succeed.
    ///
    /// `MGET` is atomic, but the chunking is not, and the result crosses a gRPC
    /// hop after this one where truncation is a live possibility. `covered`
    /// makes the guarantee checkable at the far end. e2b's equivalent guard is
    /// worth naming too: when its reconciliation pipeline errors it skips the
    /// entire round rather than acting on the part that came back — "skip
    /// entirely to avoid mass kills".
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

    async fn reserve(&self, sandbox_id: &SandboxId) -> Result<Reservation> {
        super::reserve::reserve(self.inner(), sandbox_id).await
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

        // Layer one: the callback ran long, so its result is discarded. Nothing
        // in Redis has been touched, so discarding is lossless.
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

        // Layer two: the lock has to outlive the write we are about to attempt.
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

        // Layer three: the predicates. Note the incarnation compared is the one
        // that was *read*, so a resume that installed a new one in the meantime
        // is refused rather than overwritten.
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
