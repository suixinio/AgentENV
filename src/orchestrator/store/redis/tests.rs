//! The Redis store, against a real Redis.
//!
//! Every assertion that has a control states it, because a probe with no
//! negative case proves only that the code ran. Where a control would need a
//! second replica, one is created: a single store instance cannot demonstrate
//! anything about how two of them behave, however many tasks are run against it.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use redis::AsyncCommands;
use uuid::Uuid;

use super::super::{
    MetadataStore, PausedHandle, Reservation, SandboxMetadata, StoreError, TransitionEffect,
    TransitionOutcome, TransitionRequest, TransitionSettlement,
};
use super::harness::{raw, sibling, store_or_skip};
use super::keys::{ExpiryMember, TransitionMember};
use super::RedisMetadataStore;
use crate::orchestrator::SandboxState;
use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};
use crate::types::{ExecutionId, SandboxId};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn running(id: SandboxId) -> SandboxMetadata {
    SandboxMetadata {
        id,
        state: SandboxState::Running,
        ..Default::default()
    }
}

fn capped(id: SandboxId, lifetime: Duration) -> SandboxMetadata {
    SandboxMetadata {
        id,
        state: SandboxState::Running,
        max_lifetime: Some(lifetime),
        ..Default::default()
    }
}

async fn record_pttl(store: &RedisMetadataStore, id: &SandboxId) -> i64 {
    let mut connection = raw(store);
    redis::cmd("PTTL")
        .arg(store.inner().keys().record(id))
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn expiry_members(store: &RedisMetadataStore) -> Vec<String> {
    let mut connection = raw(store);
    redis::cmd("ZRANGE")
        .arg(store.inner().keys().expiry())
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn transition_members(store: &RedisMetadataStore) -> Vec<String> {
    let mut connection = raw(store);
    redis::cmd("ZRANGE")
        .arg(store.inner().keys().transition_index())
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn stored_rev(store: &RedisMetadataStore, id: &SandboxId) -> u64 {
    store
        .inner()
        .read_record(id)
        .await
        .unwrap()
        .expect("record should exist")
        .rev
}

#[derive(Debug)]
struct FakePausedState(serde_json::Value);

impl PausedSandboxState for FakePausedState {
    fn encode(&self) -> anyhow::Result<serde_json::Value> {
        Ok(self.0.clone())
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::default()
    }
}

// ---------------------------------------------------------------------------
// the shared contract, on this backend
// ---------------------------------------------------------------------------

mod contract {
    use super::super::harness;
    use super::super::RedisMetadataStore;

    async fn new_contract_store(test: &str) -> Option<RedisMetadataStore> {
        harness::store_for(test, |_| {}).await
    }

    crate::orchestrator::store::contract::metadata_store_contract!();
}

// ---------------------------------------------------------------------------
// record shape and paused state
// ---------------------------------------------------------------------------

/// 🔴 The defect none of the design documents mentioned: `paused_state` is
/// `#[serde(skip)]`, so a naive Redis store loses it and every resume fails.
#[tokio::test]
async fn a_paused_handle_survives_the_store_as_a_reference() {
    let store = store_or_skip!("a_paused_handle_survives_the_store_as_a_reference");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Paused;
    metadata.paused_state = Some(Arc::new(FakePausedState(
        serde_json::json!({"vm": "state", "n": 7}),
    )));
    store.add(metadata).await.unwrap();

    let record = store.inner().read_record(&id).await.unwrap().unwrap();
    let reference = record
        .paused_state_ref
        .expect("the paused handle must have been encoded into a reference");
    assert_eq!(reference.state, serde_json::json!({"vm": "state", "n": 7}));

    // The control: the handle itself does not come back, and must not — under
    // `--role api` there is no factory to rebuild it, and the reference is what
    // travels to the node that owns the bytes.
    let read_back = store.get(&id).await.unwrap().unwrap();
    assert!(read_back.paused_state.is_none());
}

/// 🔴 On a shared store the answer is `Remote`, carrying the reference and the
/// node whose disk the bytes are on — never `NotPaused`.
///
/// A `--role api` replica has no backend factory and should not have one; it
/// forwards this. A `--role all` process decodes it with its own. Both need to
/// be told which of those they are looking at, and `Option<Arc<dyn ..>>` read
/// through `get` cannot tell them.
#[tokio::test]
async fn a_shared_store_answers_remote_for_a_paused_sandbox_never_not_paused() {
    let store =
        store_or_skip!("a_shared_store_answers_remote_for_a_paused_sandbox_never_not_paused");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Paused;
    metadata.paused_state = Some(Arc::new(FakePausedState(serde_json::json!({"vm": 1}))));
    store.add(metadata).await.unwrap();

    match store.paused_handle(&id).await.unwrap() {
        PausedHandle::Remote { reference, .. } => {
            assert_eq!(reference.state, serde_json::json!({"vm": 1}));
        }
        other => panic!("a shared store cannot hand back a handle; expected Remote, got {other:?}"),
    }

    // 🔴 And the plain read still returns `None`, which is exactly the reading
    // this method exists to stop anyone acting on.
    assert!(store
        .get(&id)
        .await
        .unwrap()
        .unwrap()
        .paused_state
        .is_none());
}

/// A write that does not know about placement must not erase it.
#[tokio::test]
async fn a_state_change_does_not_erase_the_paused_reference() {
    let store = store_or_skip!("a_state_change_does_not_erase_the_paused_reference");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Paused;
    metadata.paused_state = Some(Arc::new(FakePausedState(serde_json::json!({"a": 1}))));
    store.add(metadata).await.unwrap();

    store
        .update_state_if_state(&id, SandboxState::Resuming, &[SandboxState::Paused])
        .await
        .unwrap();

    let record = store.inner().read_record(&id).await.unwrap().unwrap();
    assert!(
        record.paused_state_ref.is_some(),
        "the reference to the paused bytes was dropped by an unrelated write"
    );
}

// ---------------------------------------------------------------------------
// record TTL
// ---------------------------------------------------------------------------

/// 🔴 The `KEEPTTL` probe, in the form that survives this design: the record
/// key must keep a *finite, shrinking* TTL across many writes. A bare `SET`
/// turns it into `-1`, which is "never expires" — a leak that only shows up
/// when `noeviction` starts refusing writes.
#[tokio::test]
async fn repeated_writes_leave_the_record_key_a_finite_shrinking_ttl() {
    let store = store_or_skip!(
        "repeated_writes_leave_the_record_key_a_finite_shrinking_ttl",
        |config: &mut super::RedisStoreConfig| {
            config.record_ttl_grace = Duration::from_secs(120);
        }
    );
    let id = SandboxId::new();
    store
        .add(capped(id, Duration::from_secs(600)))
        .await
        .unwrap();

    let first = record_pttl(&store, &id).await;
    assert!(
        first > 0,
        "a capped sandbox's record must have a TTL, got {first}"
    );

    // 🔴 A real interval, wider than the derivation's own granularity. The TTL
    // is rounded up to whole seconds, so two readings taken a few milliseconds
    // apart can be equal or differ by a rounding tick — and an assertion that
    // cannot fail is not an assertion. One and a bit seconds is enough for the
    // rounded value to have moved.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    for _ in 0..20 {
        store
            .update_if_state(&id, &[SandboxState::Running], |metadata| {
                metadata.set_timeout(Some(Duration::from_secs(300)));
            })
            .await
            .unwrap();
    }

    let second = record_pttl(&store, &id).await;
    assert!(
        second > 0,
        "the record key lost its TTL and will now never expire (PTTL {second})"
    );
    assert!(
        second < first,
        "a running sandbox's deadline is pinned to the start of its run, so its record's \
         TTL must only shrink: {first} -> {second}"
    );
}

/// The control for the test above, and a requirement in its own right: a
/// sandbox with no configured ceiling has no TTL, exactly as e2b's have none.
#[tokio::test]
async fn an_uncapped_sandbox_has_no_record_ttl() {
    let store = store_or_skip!("an_uncapped_sandbox_has_no_record_ttl");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.max_lifetime = None;
    store.add(metadata).await.unwrap();
    assert_eq!(record_pttl(&store, &id).await, -1);

    // And a write does not invent one.
    store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();
    assert_eq!(record_pttl(&store, &id).await, -1);
}

/// 🔴 Why the TTL is derived again on every write rather than preserved.
///
/// Paused time is free, so a paused sandbox's lifetime deadline recedes with
/// the clock. A TTL frozen at creation would eventually delete the record of a
/// sandbox that is still sitting perfectly alive on a node's disk — and a
/// record that vanishes under a live sandbox is what makes it an orphan.
#[tokio::test]
async fn a_paused_records_ttl_stops_shrinking_while_a_running_ones_does_not() {
    let store =
        store_or_skip!("a_paused_records_ttl_stops_shrinking_while_a_running_ones_does_not");

    // The subject: paused before the interval, and written again after it.
    let paused_id = SandboxId::new();
    store
        .add(capped(paused_id, Duration::from_secs(600)))
        .await
        .unwrap();
    store
        .update_state_if_state(&paused_id, SandboxState::Paused, &[SandboxState::Running])
        .await
        .unwrap();
    let paused_before = record_pttl(&store, &paused_id).await;

    // The control: identical in every way except that it keeps running.
    let running_id = SandboxId::new();
    store
        .add(capped(running_id, Duration::from_secs(600)))
        .await
        .unwrap();
    let running_before = record_pttl(&store, &running_id).await;

    // 🔴 Comfortably wider than the whole-second granularity the TTL is
    // derived at, so the running record's value has to have moved.
    tokio::time::sleep(Duration::from_millis(2_100)).await;

    store
        .update_if_state(&paused_id, &[SandboxState::Paused], |metadata| {
            metadata.snapshot_id = "touched".to_string();
        })
        .await
        .unwrap();
    store
        .update_if_state(&running_id, &[SandboxState::Running], |metadata| {
            metadata.snapshot_id = "touched".to_string();
        })
        .await
        .unwrap();

    let paused_after = record_pttl(&store, &paused_id).await;
    let running_after = record_pttl(&store, &running_id).await;

    // 🔴 The property, stated where it cannot be confused with a rounding
    // boundary: a paused sandbox spends nothing, so its deadline recedes with
    // the clock and its record's TTL holds. A frozen TTL would eventually
    // delete the record of a sandbox still sitting alive on a node's disk —
    // and a record that vanishes under a live sandbox makes it an orphan.
    assert!(
        paused_after >= paused_before - 1_000,
        "a paused record's TTL kept draining: {paused_before} -> {paused_after}"
    );
    assert!(
        running_after <= running_before - 1_000,
        "a running record's deadline is pinned, so its TTL must drain: \
         {running_before} -> {running_after}"
    );
}

// ---------------------------------------------------------------------------
// the incarnation predicate
// ---------------------------------------------------------------------------

/// 🔴 The predicate that carries correctness. e2b's own `Update` is a bare
/// `SET` with none, on a path where a lockless `add` can install a new
/// incarnation at any moment.
#[tokio::test]
async fn a_write_for_a_superseded_incarnation_is_refused() {
    let store = store_or_skip!("a_write_for_a_superseded_incarnation_is_refused");
    let id = SandboxId::new();
    let original = running(id);
    store.add(original.clone()).await.unwrap();

    // A resume replaces the record with a new incarnation, exactly as
    // `restore_sandbox` does — losslessly, and without any lock.
    store.remove(&id).await.unwrap();
    let mut reborn = running(id);
    reborn.execution_id = ExecutionId::new();
    store.add(reborn.clone()).await.unwrap();

    // The caller is still holding the record it read before all that.
    let mut stale = original.clone();
    stale.snapshot_id = "written-by-the-dead-incarnation".to_string();
    let error = store.update(stale).await.unwrap_err();
    match error {
        StoreError::ExecutionSuperseded {
            expected, actual, ..
        } => {
            assert_eq!(expected, original.execution_id);
            assert_eq!(actual, Some(reborn.execution_id));
        }
        other => panic!("expected the write to be fenced, got {other:?}"),
    }

    // And the live record is untouched.
    let live = store.get(&id).await.unwrap().unwrap();
    assert_eq!(live.execution_id, reborn.execution_id);
    assert_ne!(live.snapshot_id, "written-by-the-dead-incarnation");
}

/// The control: the very same write, for the incarnation that is actually
/// live, goes through. Without this, the test above would pass just as well
/// against a store that refused every write.
#[tokio::test]
async fn a_write_for_the_live_incarnation_goes_through() {
    let store = store_or_skip!("a_write_for_the_live_incarnation_goes_through");
    let id = SandboxId::new();
    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();

    let mut next = metadata;
    next.snapshot_id = "written-by-the-live-incarnation".to_string();
    store.update(next).await.unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().snapshot_id,
        "written-by-the-live-incarnation"
    );
}

/// Replays the exact race `scripts.go` describes: a caller reads a record, a
/// lockless `add` installs a new incarnation, and the caller's write lands
/// afterwards. Returns the incarnation left in the store.
///
/// 🔴 Driven through `write_record` rather than through `update`, on purpose.
/// `update` also compares incarnations in Rust *before* the script, which is
/// worth having but is a read-then-write and therefore not the guard under
/// test. Going through it here would have the Rust check refuse the write and
/// the test would pass with the script predicates removed — proving nothing.
async fn replay_the_lockless_add_race(store: &RedisMetadataStore) -> ExecutionId {
    let id = SandboxId::new();
    let original = running(id);
    store.add(original.clone()).await.unwrap();
    let read = store.inner().read_record(&id).await.unwrap().unwrap();

    store.remove(&id).await.unwrap();
    let mut reborn = running(id);
    reborn.execution_id = ExecutionId::new();
    store.add(reborn).await.unwrap();

    let mut stale = read.clone();
    stale.metadata.snapshot_id = "written-by-the-dead-incarnation".to_string();
    stale.rev += 1;
    let _ = store
        .inner()
        .write_record(
            &read.metadata,
            &stale,
            read.rev,
            Some(read.execution_id()),
            super::crud::TtlMode::Recompute,
        )
        .await;

    store.get(&id).await.unwrap().unwrap().execution_id
}

/// With the predicates in place, the stale write is refused and the live
/// incarnation stands.
#[tokio::test]
async fn the_script_predicates_refuse_a_write_from_a_replaced_incarnation() {
    let store = store_or_skip!("the_script_predicates_refuse_a_write_from_a_replaced_incarnation");
    let id = SandboxId::new();
    let original = running(id);
    store.add(original.clone()).await.unwrap();
    let read = store.inner().read_record(&id).await.unwrap().unwrap();

    store.remove(&id).await.unwrap();
    let mut reborn = running(id);
    reborn.execution_id = ExecutionId::new();
    store.add(reborn.clone()).await.unwrap();

    let mut stale = read.clone();
    stale.metadata.snapshot_id = "written-by-the-dead-incarnation".to_string();
    stale.rev += 1;
    let error = store
        .inner()
        .write_record(
            &read.metadata,
            &stale,
            read.rev,
            Some(read.execution_id()),
            super::crud::TtlMode::Recompute,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::ExecutionSuperseded { .. }),
        "{error:?}"
    );
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().execution_id,
        reborn.execution_id
    );
}

/// 🔴 The negative control for the whole design. With the predicates removed
/// from the script, the identical sequence overwrites the live incarnation with
/// a dead one — which is what says the predicates are load-bearing rather than
/// decorative, and that the lock is not what was doing the work.
#[tokio::test]
async fn without_the_predicates_a_stale_write_overwrites_the_live_record() {
    let with_predicates =
        store_or_skip!("without_the_predicates_a_stale_write_overwrites_the_live_record");
    let survivor_with = replay_the_lockless_add_race(&with_predicates).await;

    let without = store_or_skip!(
        "without_the_predicates_a_stale_write_overwrites_the_live_record",
        |config: &mut super::RedisStoreConfig| {
            config.cas_predicates_enabled = false;
        }
    );
    let survivor_without = replay_the_lockless_add_race(&without).await;

    assert_ne!(
        survivor_with, survivor_without,
        "the two arms produced the same outcome, so the predicates are not what decides it"
    );
}

// ---------------------------------------------------------------------------
// the revision predicate and the closure budget
// ---------------------------------------------------------------------------

/// 🔴 The closure budget has teeth, and nothing is written when it bites.
#[tokio::test]
async fn a_callback_that_overruns_its_budget_writes_nothing() {
    let store = store_or_skip!(
        "a_callback_that_overruns_its_budget_writes_nothing",
        |config: &mut super::RedisStoreConfig| {
            config.closure_budget = Duration::from_millis(20);
        }
    );
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let before = stored_rev(&store, &id).await;

    let error = store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            // A blocking call inside the callback is precisely what the budget
            // is there to catch.
            std::thread::sleep(Duration::from_millis(200));
            metadata.snapshot_id = "should-not-land".to_string();
        })
        .await
        .unwrap_err();

    match error {
        StoreError::ClosureBudgetExceeded { elapsed, .. } => {
            assert!(elapsed >= Duration::from_millis(200), "{elapsed:?}");
        }
        other => panic!("expected the budget to bite, got {other:?}"),
    }
    assert_eq!(
        stored_rev(&store, &id).await,
        before,
        "the record must be untouched when the callback is discarded"
    );
    assert_ne!(
        store.get(&id).await.unwrap().unwrap().snapshot_id,
        "should-not-land"
    );
}

/// The control: the same call with a callback that finishes in time succeeds
/// and advances the revision. Without it, the assertion above could just as
/// well be measuring some unrelated failure.
#[tokio::test]
async fn a_callback_within_its_budget_is_written() {
    let store = store_or_skip!(
        "a_callback_within_its_budget_is_written",
        |config: &mut super::RedisStoreConfig| {
            // 🔴 The pair varies the *budget*, not the callback: both arms sleep
            // for the same 200ms. Making the control's callback fast instead
            // would have made it a race against whatever else the machine is
            // doing, which is a control that fails for reasons unrelated to
            // what it is controlling for.
            config.closure_budget = Duration::from_secs(5);
        }
    );
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let before = stored_rev(&store, &id).await;

    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            std::thread::sleep(Duration::from_millis(200));
            metadata.snapshot_id = "landed".to_string();
        })
        .await
        .unwrap();

    assert_eq!(stored_rev(&store, &id).await, before + 1);
    assert_eq!(store.get(&id).await.unwrap().unwrap().snapshot_id, "landed");
}

/// 🔴 The revision predicate, with the lock switched off so the race is real.
///
/// Turning the lock off is safe: it is the throughput mechanism, not the
/// correctness one. What it changes is that a losing writer now finds out by
/// being refused, instead of by waiting.
#[tokio::test]
async fn a_stale_revision_loses_the_write_rather_than_overwriting_it() {
    let store = store_or_skip!(
        "a_stale_revision_loses_the_write_rather_than_overwriting_it",
        |config: &mut super::RedisStoreConfig| {
            config.distributed_lock_enabled = false;
        }
    );
    let id = SandboxId::new();
    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();

    let record = store.inner().read_record(&id).await.unwrap().unwrap();

    // Somebody else writes first, advancing the revision.
    store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();

    // Now replay the write the first reader was about to make, at its old
    // revision. It must lose.
    let mut next = record.clone();
    next.metadata.snapshot_id = "stale".to_string();
    next.rev += 1;
    let error = store
        .inner()
        .write_record(
            &record.metadata,
            &next,
            record.rev,
            Some(record.execution_id()),
            super::crud::TtlMode::Recompute,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::ConcurrentUpdate { .. }),
        "{error:?}"
    );
    assert_ne!(store.get(&id).await.unwrap().unwrap().snapshot_id, "stale");
}

// ---------------------------------------------------------------------------
// the expiry index
// ---------------------------------------------------------------------------

/// 🔴 Members are scoped to the incarnation, and this is what that buys.
///
/// A `ZREM` aimed at a dead incarnation cannot unindex the live one. Without
/// the scoping, the two members would be the same string, and cleaning up after
/// the dead run would silently make the live sandbox unable to expire — for
/// ever, because nothing else puts it back.
#[tokio::test]
async fn removing_a_dead_incarnations_expiry_member_leaves_the_live_one_indexed() {
    let store =
        store_or_skip!("removing_a_dead_incarnations_expiry_member_leaves_the_live_one_indexed");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.set_timeout(Some(Duration::from_secs(3600)));
    let first_execution = metadata.execution_id;
    store.add(metadata).await.unwrap();

    // A resume installs a new incarnation through the closure update, which is
    // the path e2b never takes and therefore never had to rescore for.
    let second_execution = ExecutionId::new();
    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.execution_id = second_execution;
        })
        .await
        .unwrap();

    let dead = ExpiryMember::new(id, first_execution).encode();
    let live = ExpiryMember::new(id, second_execution).encode();
    // 🔴 The control that makes the rest of this test mean anything: with a
    // sandbox-id-only member these two would be the same string, and the `ZREM`
    // below would hit the live entry.
    assert_ne!(dead, live);

    let members = expiry_members(&store).await;
    assert!(
        members.contains(&live),
        "the live incarnation must be indexed"
    );
    assert!(
        !members.contains(&dead),
        "the rescore should already have removed the dead member"
    );

    // Now do it again explicitly, as a stale sweep would.
    let mut connection = raw(&store);
    let _: i64 = connection
        .zrem(store.inner().keys().expiry(), &dead)
        .await
        .unwrap();

    assert!(expiry_members(&store).await.contains(&live));

    // And the sandbox still expires.
    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
        })
        .await
        .unwrap();
    let expired = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert!(expired.iter().any(|record| record.id == id));
}

/// A removal builds its `ZREM` from the record it just deleted, not from
/// whatever the caller was holding.
#[tokio::test]
async fn removing_a_record_unindexes_the_incarnation_that_was_actually_stored() {
    let store =
        store_or_skip!("removing_a_record_unindexes_the_incarnation_that_was_actually_stored");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.set_timeout(Some(Duration::from_secs(60)));
    store.add(metadata).await.unwrap();
    assert_eq!(expiry_members(&store).await.len(), 1);

    store.remove(&id).await.unwrap();
    assert!(expiry_members(&store).await.is_empty());
}

/// Garbage in the index is swept as garbage, never mistaken for a sandbox.
#[tokio::test]
async fn unparseable_index_members_are_swept() {
    let store = store_or_skip!("unparseable_index_members_are_swept");
    let mut connection = raw(&store);
    let _: i64 = connection
        .zadd(store.inner().keys().expiry(), "not-a-member", 1i64)
        .await
        .unwrap();

    let expired = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert!(expired.is_empty());
    assert!(expiry_members(&store).await.is_empty());
}

/// A member whose record has expired out from under it is an orphan, and an
/// orphan is swept rather than reported as an expired sandbox.
#[tokio::test]
async fn orphan_members_are_swept_rather_than_evicted() {
    let store = store_or_skip!("orphan_members_are_swept_rather_than_evicted");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
    store.add(metadata).await.unwrap();

    let mut connection = raw(&store);
    let _: i64 = connection
        .del(store.inner().keys().record(&id))
        .await
        .unwrap();

    assert!(store
        .expired_batch(SystemTime::now(), 10)
        .await
        .unwrap()
        .is_empty());
    assert!(expiry_members(&store).await.is_empty());
}

/// A record whose expiry moved out is rescored rather than evicted — and the
/// rescore is `XX`, so it can never resurrect a member a concurrent removal
/// deleted.
#[tokio::test]
async fn a_record_that_is_no_longer_due_is_rescored_not_evicted() {
    let store = store_or_skip!("a_record_that_is_no_longer_due_is_rescored_not_evicted");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() + Duration::from_secs(3600));
    store.add(metadata.clone()).await.unwrap();

    // Backdate the index entry without touching the record, which is what a
    // healer race or a clock skew looks like.
    let member = ExpiryMember::new(id, metadata.execution_id).encode();
    let mut connection = raw(&store);
    let _: i64 = connection
        .zadd(store.inner().keys().expiry(), &member, 1i64)
        .await
        .unwrap();

    let expired = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert!(expired.is_empty(), "the record is not actually due");

    let score: Option<f64> = connection
        .zscore(store.inner().keys().expiry(), &member)
        .await
        .unwrap();
    assert!(score.unwrap() > 1.0, "the member should have been rescored");
}

/// 🔴 The `XX` on the rescore, asserted directly.
///
/// A member that is present is moved; a member that is not present is **not**
/// created. Without `XX` the second half would fail, and what it would create
/// is an index entry for a sandbox a concurrent `remove` has just deleted —
/// planted by the sweep whose whole job is to take such entries out.
#[tokio::test]
async fn rescoring_moves_existing_members_and_resurrects_none() {
    let store = store_or_skip!("rescoring_moves_existing_members_and_resurrects_none");
    let key = store.inner().keys().expiry();
    let mut connection = raw(&store);
    let _: i64 = connection.zadd(&key, "present", 1i64).await.unwrap();

    super::expiry::rescore_existing_members(
        &mut connection,
        &key,
        &[
            (500i64, "present".to_string()),
            (500i64, "absent".to_string()),
        ],
    )
    .await
    .unwrap();

    let present: Option<f64> = connection.zscore(&key, "present").await.unwrap();
    assert_eq!(present, Some(500.0), "an existing member must be rescored");
    let absent: Option<f64> = connection.zscore(&key, "absent").await.unwrap();
    assert_eq!(
        absent, None,
        "a member that was not there must not be created by a rescore"
    );
}

/// 🔴 The second layer of the closure budget: the write is abandoned when the
/// lock will not outlive it, even though the callback itself finished in time.
///
/// This is the layer that catches what a callback timer cannot — a GC pause,
/// scheduler starvation, a stalled Redis. The callback here is deliberately
/// well inside its own budget.
#[tokio::test]
async fn a_write_is_abandoned_when_its_lock_will_not_outlive_it() {
    let store = store_or_skip!(
        "a_write_is_abandoned_when_its_lock_will_not_outlive_it",
        |config: &mut super::RedisStoreConfig| {
            config.closure_budget = Duration::from_secs(5);
            // Leaves 100ms of usable lock, which a 300ms callback overruns.
            config.write_budget = config.lock_ttl - Duration::from_millis(100);
        }
    );
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let before = stored_rev(&store, &id).await;

    let error = store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            std::thread::sleep(Duration::from_millis(300));
            metadata.snapshot_id = "should-not-land".to_string();
        })
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::LockLapsed { .. }), "{error:?}");
    assert_eq!(stored_rev(&store, &id).await, before);

    // The control: the identical callback with room on the lock is written.
    let roomy = super::harness::store_for(
        "a_write_is_abandoned_when_its_lock_will_not_outlive_it",
        |config| {
            config.closure_budget = Duration::from_secs(5);
            config.write_budget = Duration::from_millis(100);
        },
    )
    .await
    .unwrap();
    let other_id = SandboxId::new();
    roomy.add(running(other_id)).await.unwrap();
    roomy
        .update_if_state(&other_id, &[SandboxState::Running], |metadata| {
            std::thread::sleep(Duration::from_millis(300));
            metadata.snapshot_id = "landed".to_string();
        })
        .await
        .unwrap();
    assert_eq!(
        roomy.get(&other_id).await.unwrap().unwrap().snapshot_id,
        "landed"
    );
}

/// The healer puts back an entry that should exist and does not, and does not
/// touch one that is merely young.
#[tokio::test]
async fn the_healer_repairs_a_missing_entry_and_skips_a_young_one() {
    let store = store_or_skip!(
        "the_healer_repairs_a_missing_entry_and_skips_a_young_one",
        |config: &mut super::RedisStoreConfig| {
            config.heal_grace = Duration::from_secs(60);
        }
    );

    let old = SandboxId::new();
    let mut metadata = running(old);
    metadata.created_at = SystemTime::now() - Duration::from_secs(600);
    metadata.expires_at = Some(SystemTime::now() + Duration::from_secs(600));
    store.add(metadata.clone()).await.unwrap();

    let young = SandboxId::new();
    let mut metadata_young = running(young);
    metadata_young.expires_at = Some(SystemTime::now() + Duration::from_secs(600));
    store.add(metadata_young.clone()).await.unwrap();

    // Lose both entries, as a partial write or an operator mistake would.
    let mut connection = raw(&store);
    let _: i64 = connection.del(store.inner().keys().expiry()).await.unwrap();

    let healed = store.heal_expiry_index().await.unwrap();
    assert_eq!(healed, 1, "only the settled record should be repaired");

    let members = expiry_members(&store).await;
    assert!(members.contains(&ExpiryMember::new(old, metadata.execution_id).encode()));
    assert!(
        !members.contains(&ExpiryMember::new(young, metadata_young.execution_id).encode()),
        "a record still inside its grace period must be left alone"
    );
}

/// 🔴 The positive form of the `timeout = None` hole: a record with no expiry
/// still gets a coordinate, taken from its lifetime ceiling.
#[tokio::test]
async fn the_healer_indexes_a_record_that_has_no_expiry_at_its_lifetime_deadline() {
    let store =
        store_or_skip!("the_healer_indexes_a_record_that_has_no_expiry_at_its_lifetime_deadline");
    let id = SandboxId::new();
    let mut metadata = capped(id, Duration::from_secs(3600));
    metadata.created_at = SystemTime::now() - Duration::from_secs(600);
    metadata.expires_at = None;
    store.add(metadata.clone()).await.unwrap();
    assert!(expiry_members(&store).await.is_empty());

    assert_eq!(store.heal_expiry_index().await.unwrap(), 1);
    assert!(expiry_members(&store)
        .await
        .contains(&ExpiryMember::new(id, metadata.execution_id).encode()));
}

/// The healer's kill switch is read every round, and reports being off rather
/// than reporting having found nothing.
#[tokio::test]
async fn the_healer_can_be_switched_off() {
    let store = store_or_skip!(
        "the_healer_can_be_switched_off",
        |config: &mut super::RedisStoreConfig| {
            config.expiry_healer_enabled = false;
            config.heal_grace = Duration::ZERO;
        }
    );
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() + Duration::from_secs(600));
    store.add(metadata).await.unwrap();
    let mut connection = raw(&store);
    let _: i64 = connection.del(store.inner().keys().expiry()).await.unwrap();

    assert_eq!(store.heal_expiry_index().await.unwrap(), 0);
    assert!(expiry_members(&store).await.is_empty());
}

// ---------------------------------------------------------------------------
// transitions
// ---------------------------------------------------------------------------

async fn start_pause(store: &RedisMetadataStore, id: &SandboxId) -> TransitionOutcome {
    store
        .start_transition(
            id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_effect(TransitionEffect::Terminal(SandboxState::Paused)),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn a_transition_publishes_a_key_an_index_entry_and_a_result() {
    let store = store_or_skip!("a_transition_publishes_a_key_an_index_entry_and_a_result");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the first transition must start");
    };
    let transition_id = guard.transition_id().to_string();

    let mut connection = raw(&store);
    let held: Option<String> = connection
        .get(store.inner().keys().transition(&id))
        .await
        .unwrap();
    assert_eq!(held.as_deref(), Some(transition_id.as_str()));
    let ttl: i64 = redis::cmd("TTL")
        .arg(store.inner().keys().transition(&id))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(ttl > 0, "the transition key must carry a TTL, got {ttl}");
    assert_eq!(transition_members(&store).await.len(), 1);
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Pausing
    );

    // A joiner can see it is still running.
    assert_eq!(
        store
            .transition_settlement(&id, &transition_id)
            .await
            .unwrap(),
        TransitionSettlement::Running
    );

    guard.complete(Ok(())).await.unwrap();

    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Paused
    );
    let held: Option<String> = connection
        .get(store.inner().keys().transition(&id))
        .await
        .unwrap();
    assert!(held.is_none(), "the transition key must be released");
    // 🔴 And the index entry goes with it. A queue that only empties on failure
    // is how a build system came to refuse every build cluster-wide after
    // enough *successes*.
    assert!(transition_members(&store).await.is_empty());
    assert_eq!(
        store
            .transition_settlement(&id, &transition_id)
            .await
            .unwrap(),
        TransitionSettlement::Settled(Ok(()))
    );
}

#[tokio::test]
async fn a_failed_transition_rolls_the_state_back_and_reports_why() {
    let store = store_or_skip!("a_failed_transition_rolls_the_state_back_and_reports_why");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };
    let transition_id = guard.transition_id().to_string();
    guard
        .complete(Err("the vm would not stop".to_string()))
        .await
        .unwrap();

    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Running,
        "a failed pause returns the sandbox to the state it came from"
    );
    assert_eq!(
        store
            .transition_settlement(&id, &transition_id)
            .await
            .unwrap(),
        TransitionSettlement::Settled(Err("the vm would not stop".to_string()))
    );
}

/// 🔴 Three answers, not two. A transition whose key expired with no result
/// behind it is the owner having died — reporting it as success would report an
/// operation complete that never happened.
#[tokio::test]
async fn a_transition_whose_owner_vanished_is_not_reported_as_success() {
    let store = store_or_skip!("a_transition_whose_owner_vanished_is_not_reported_as_success");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };
    let transition_id = guard.transition_id().to_string();
    // The owning replica dies: no completion, and the key eventually expires.
    std::mem::forget(guard);
    let mut connection = raw(&store);
    let _: i64 = connection
        .del(store.inner().keys().transition(&id))
        .await
        .unwrap();

    assert_eq!(
        store
            .transition_settlement(&id, &transition_id)
            .await
            .unwrap(),
        TransitionSettlement::OwnerVanished
    );
}

#[tokio::test]
async fn a_second_transition_towards_the_same_state_joins_rather_than_conflicts() {
    let store =
        store_or_skip!("a_second_transition_towards_the_same_state_joins_rather_than_conflicts");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the first transition must start");
    };
    let first = guard.transition_id().to_string();

    // A second replica asks for the same thing.
    let other = sibling(&store).await;
    match other
        .start_transition(
            &id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_effect(TransitionEffect::Terminal(SandboxState::Paused)),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::InFlight { transition_id } => assert_eq!(transition_id, first),
        other => panic!("expected to join the transition in flight, got {other:?}"),
    }
    guard.complete(Ok(())).await.unwrap();
}

#[tokio::test]
async fn an_illegal_transition_is_refused_as_illegal_not_as_a_conflict() {
    let store = store_or_skip!("an_illegal_transition_is_refused_as_illegal_not_as_a_conflict");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let error = store
        .start_transition(
            &id,
            // There is no edge from `Running` back to `Creating`; only `add`
            // produces that state.
            TransitionRequest::new(SandboxState::Creating, vec![SandboxState::Running]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::InvalidTransition { .. }),
        "an edge that does not exist is not the same as arriving a step late: {error:?}"
    );
}

/// 🔴 The eviction re-check, atomic with the state write.
///
/// This is a defect the in-process store has today: the evictor reads the
/// expiry index, then compare-and-sets on **state**, so a `keep_alive` landing
/// in between pushes the expiry out and the sandbox is paused anyway. Redis
/// only widens that window; the fix is to make expiry part of the same atomic
/// step.
#[tokio::test]
async fn an_eviction_refuses_a_sandbox_that_was_kept_alive_in_the_meantime() {
    let store = store_or_skip!("an_eviction_refuses_a_sandbox_that_was_kept_alive_in_the_meantime");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
    store.add(metadata).await.unwrap();

    // The evictor has read the index and is about to act.
    let due = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert_eq!(due.len(), 1);

    // A keep-alive lands first.
    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.set_timeout(Some(Duration::from_secs(3600)));
        })
        .await
        .unwrap();

    let outcome = store
        .start_transition(
            &id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_effect(TransitionEffect::Terminal(SandboxState::Paused))
                .as_eviction(),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, TransitionOutcome::NotExpired),
        "the sandbox is no longer due and must not be evicted: {outcome:?}"
    );
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Running
    );
}

/// The control: without the keep-alive, the same eviction starts. And the
/// second control: the same request without the eviction flag starts even for a
/// sandbox that is not due, which is what says the flag is what did the work.
#[tokio::test]
async fn an_eviction_of_a_sandbox_that_is_still_due_starts() {
    let store = store_or_skip!("an_eviction_of_a_sandbox_that_is_still_due_starts");
    let due_id = SandboxId::new();
    let mut metadata = running(due_id);
    metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
    store.add(metadata).await.unwrap();

    let outcome = store
        .start_transition(
            &due_id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_effect(TransitionEffect::Terminal(SandboxState::Paused))
                .as_eviction(),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, TransitionOutcome::Started(_)),
        "{outcome:?}"
    );

    let live_id = SandboxId::new();
    let mut metadata = running(live_id);
    metadata.expires_at = Some(SystemTime::now() + Duration::from_secs(3600));
    store.add(metadata).await.unwrap();
    let outcome = store
        .start_transition(
            &live_id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_effect(TransitionEffect::Terminal(SandboxState::Paused)),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, TransitionOutcome::Started(_)),
        "a plain pause does not consult the expiry: {outcome:?}"
    );
}

#[tokio::test]
async fn a_transition_for_a_superseded_incarnation_is_refused() {
    let store = store_or_skip!("a_transition_for_a_superseded_incarnation_is_refused");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let error = store
        .start_transition(
            &id,
            TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
                .with_execution(ExecutionId::new()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::ExecutionSuperseded { .. }),
        "{error:?}"
    );
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Running,
        "a refused transition must not have moved the state"
    );
}

// ---------------------------------------------------------------------------
// the reaper
// ---------------------------------------------------------------------------

/// 🔴 The hole e2b's design cannot cover: a sandbox with `timeout = None`,
/// stuck in `Pausing` because the replica pausing it died. It is in no expiry
/// index, so no sweep will ever look at it, and to the user it is a sandbox
/// that cannot be deleted.
#[tokio::test]
async fn a_stuck_transition_on_a_sandbox_with_no_timeout_is_reaped() {
    let store = store_or_skip!("a_stuck_transition_on_a_sandbox_with_no_timeout_is_reaped");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.timeout = None;
    metadata.expires_at = None;
    metadata.max_lifetime = None;
    store.add(metadata).await.unwrap();
    assert!(expiry_members(&store).await.is_empty());

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };
    // The replica dies: no completion, and the key expires.
    std::mem::forget(guard);
    let mut connection = raw(&store);
    let _: i64 = connection
        .del(store.inner().keys().transition(&id))
        .await
        .unwrap();

    // A round in the future, past the index deadline.
    let reaped = store
        .reap_stuck_transitions(SystemTime::now() + Duration::from_secs(600))
        .await
        .unwrap();
    assert_eq!(reaped, vec![id]);

    // 🔴 The record is not guessed back to `Running`. It is given a coordinate
    // and handed to the evictor, which goes down the full path and asks the
    // node what actually happened to the VM.
    let record = store.get(&id).await.unwrap().unwrap();
    assert_eq!(record.state, SandboxState::Pausing);
    assert!(
        record.expires_at.is_some(),
        "the record must now be findable"
    );
    assert!(!expiry_members(&store).await.is_empty());
    assert!(transition_members(&store).await.is_empty());
}

/// The control: with the reaper switched off, the same sandbox stays stuck for
/// ever. This is what says the reaper is what freed it.
#[tokio::test]
async fn with_the_reaper_off_a_stuck_transition_stays_stuck() {
    let store = store_or_skip!(
        "with_the_reaper_off_a_stuck_transition_stays_stuck",
        |config: &mut super::RedisStoreConfig| {
            config.transition_reaper_enabled = false;
        }
    );
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = None;
    metadata.max_lifetime = None;
    store.add(metadata).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };
    std::mem::forget(guard);
    let mut connection = raw(&store);
    let _: i64 = connection
        .del(store.inner().keys().transition(&id))
        .await
        .unwrap();

    let reaped = store
        .reap_stuck_transitions(SystemTime::now() + Duration::from_secs(600))
        .await
        .unwrap();
    assert!(reaped.is_empty());
    let record = store.get(&id).await.unwrap().unwrap();
    assert_eq!(record.state, SandboxState::Pausing);
    assert!(record.expires_at.is_none());
}

/// A transition that is simply still running is left alone.
#[tokio::test]
async fn the_reaper_leaves_a_live_transition_alone() {
    let store = store_or_skip!("the_reaper_leaves_a_live_transition_alone");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };

    let reaped = store
        .reap_stuck_transitions(SystemTime::now() + Duration::from_secs(600))
        .await
        .unwrap();
    assert!(
        reaped.is_empty(),
        "the transition key is still held, so the deadline was simply computed early"
    );
    guard.complete(Ok(())).await.unwrap();
}

/// 🔴 Three segments, and this is why: a reaper acting on a dead transition
/// must not unindex a live one started on the same sandbox afterwards.
#[tokio::test]
async fn the_reaper_drops_members_for_dead_incarnations_and_missing_records() {
    let store =
        store_or_skip!("the_reaper_drops_members_for_dead_incarnations_and_missing_records");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let mut connection = raw(&store);
    let key = store.inner().keys().transition_index();
    // A member for an incarnation that is not the live one.
    let dead = TransitionMember::new(id, ExecutionId::new(), Uuid::now_v7()).encode();
    // A member for a sandbox that no longer exists at all.
    let orphan =
        TransitionMember::new(SandboxId::new(), ExecutionId::new(), Uuid::now_v7()).encode();
    let _: i64 = connection.zadd(&key, &dead, 1i64).await.unwrap();
    let _: i64 = connection.zadd(&key, &orphan, 1i64).await.unwrap();
    let _: i64 = connection.zadd(&key, "rubbish", 1i64).await.unwrap();

    let reaped = store
        .reap_stuck_transitions(SystemTime::now())
        .await
        .unwrap();
    assert!(reaped.is_empty(), "none of these name a stuck sandbox");
    assert!(transition_members(&store).await.is_empty());
    // And the live record is untouched.
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Running
    );
}

// ---------------------------------------------------------------------------
// reservations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_reservation_moves_through_its_three_states() {
    let store = store_or_skip!("a_reservation_moves_through_its_three_states");
    let id = SandboxId::new();

    // 1. Nobody has this id.
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the first caller must get the window");
    };

    // 2. A second replica finds it pending, and is told to wait rather than
    //    given a conflict.
    let other = sibling(&store).await;
    let Reservation::AlreadyPending(waiter) = other.reserve(&id).await.unwrap() else {
        panic!("the second caller must find the window open");
    };

    // The creation lands, then the window closes.
    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();
    guard.finish(Ok(())).await.unwrap();

    let seen = tokio::time::timeout(Duration::from_secs(5), waiter.wait())
        .await
        .expect("the waiter should have woken")
        .unwrap();
    assert_eq!(seen.id, id);
    assert_eq!(seen.execution_id, metadata.execution_id);

    // 3. And now the sandbox simply exists.
    assert!(matches!(
        store.reserve(&id).await.unwrap(),
        Reservation::AlreadyInStorage
    ));
}

/// 🔴 A creation that lands must free its slot. A queue that only empties on
/// failure is the shape of a defect that eventually refuses everything.
#[tokio::test]
async fn adding_the_record_closes_the_creation_window() {
    let store = store_or_skip!("adding_the_record_closes_the_creation_window");
    let id = SandboxId::new();
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the window should open");
    };

    let mut connection = raw(&store);
    let pending: Option<f64> = connection
        .zscore(store.inner().keys().pending(), id.to_string())
        .await
        .unwrap();
    assert!(pending.is_some());

    store.add(running(id)).await.unwrap();

    let pending: Option<f64> = connection
        .zscore(store.inner().keys().pending(), id.to_string())
        .await
        .unwrap();
    assert!(
        pending.is_none(),
        "the record landed, so the creation window must be closed"
    );
    guard.finish(Ok(())).await.unwrap();
}

#[tokio::test]
async fn a_failed_creation_reports_its_failure_to_the_waiter() {
    let store = store_or_skip!("a_failed_creation_reports_its_failure_to_the_waiter");
    let id = SandboxId::new();
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the window should open");
    };
    let Reservation::AlreadyPending(waiter) = store.reserve(&id).await.unwrap() else {
        panic!("the second caller should wait");
    };

    guard
        .finish(Err("the image could not be pulled".to_string()))
        .await
        .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(5), waiter.wait())
        .await
        .expect("the waiter should have woken")
        .unwrap_err();
    assert!(
        error.to_string().contains("the image could not be pulled"),
        "{error}"
    );
}

/// 🔴 The reason this primitive exists here at all: the pending entry is the
/// only cluster-visible evidence that a VM is being built. Without it a
/// reconciliation sweep sees a running VM with no record and kills it.
#[tokio::test]
async fn a_creation_window_is_visible_before_the_record_exists() {
    let store = store_or_skip!("a_creation_window_is_visible_before_the_record_exists");
    let id = SandboxId::new();
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the window should open");
    };

    // From another replica's point of view: no record...
    let other = sibling(&store).await;
    assert!(other.get(&id).await.unwrap().is_none());
    let rows = other.get_many(&[id]).await.unwrap();
    assert!(rows.entries.is_empty());
    assert!(rows.covers(&[id]), "the read did cover this id");

    // ...but the id is not free either, which is the difference between "there
    // is nothing here" and "there is nothing here yet".
    assert!(matches!(
        other.reserve(&id).await.unwrap(),
        Reservation::AlreadyPending(_)
    ));
    guard.finish(Ok(())).await.unwrap();
}

/// An abandoned window is eventually released, so an id cannot be lost for ever
/// to a replica that died mid-creation.
#[tokio::test]
async fn a_stale_creation_window_is_released() {
    let store = store_or_skip!(
        "a_stale_creation_window_is_released",
        |config: &mut super::RedisStoreConfig| {
            config.reserve_stale_ttl = Duration::from_secs(1);
        }
    );
    let id = SandboxId::new();
    let mut connection = raw(&store);
    // A window opened well in the past.
    let long_ago = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 3600;
    let _: i64 = connection
        .zadd(store.inner().keys().pending(), id.to_string(), long_ago)
        .await
        .unwrap();

    assert!(matches!(
        store.reserve(&id).await.unwrap(),
        Reservation::Reserved(_)
    ));
}

// ---------------------------------------------------------------------------
// listings and coverage
// ---------------------------------------------------------------------------

/// A record that expired out from under its membership entry is dropped from
/// the roster rather than reported, because a roster entry with no record is
/// what makes a live VM look like an orphan.
#[tokio::test]
async fn a_membership_entry_with_no_record_is_swept_from_the_roster() {
    let store = store_or_skip!("a_membership_entry_with_no_record_is_swept_from_the_roster");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let mut connection = raw(&store);
    let _: i64 = connection
        .del(store.inner().keys().record(&id))
        .await
        .unwrap();

    assert!(store.list_ids().await.unwrap().is_empty());
    assert!(store.list().await.unwrap().is_empty());
    let members: Vec<String> = connection
        .smembers(store.inner().keys().index())
        .await
        .unwrap();
    assert!(members.is_empty());
}

/// 🔴 The sweep's predicate, tested directly rather than through a race.
///
/// The membership entry may only be dropped when the record really is gone,
/// and the check has to be inside the script: a check made in Rust and an
/// `SREM` issued afterwards lets a lockless `add` land in between and have its
/// brand-new sandbox unindexed. A test that only sweeps an entry whose record
/// is already gone cannot tell the two arrangements apart, so this one asks the
/// sweep to remove an entry whose record is very much present.
#[tokio::test]
async fn the_sweep_refuses_to_unindex_a_record_that_exists() {
    let store = store_or_skip!("the_sweep_refuses_to_unindex_a_record_that_exists");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    store.inner().sweep_index_member(&id).await.unwrap();

    let mut connection = raw(&store);
    let members: Vec<String> = connection
        .smembers(store.inner().keys().index())
        .await
        .unwrap();
    assert_eq!(
        members,
        vec![id.to_string()],
        "the sweep unindexed a sandbox whose record is still there"
    );
    assert_eq!(store.list_ids().await.unwrap(), vec![id]);
}

/// Coverage across several chunks, which is where a truncation would hide.
#[tokio::test]
async fn coverage_is_reported_across_chunk_boundaries() {
    let store = store_or_skip!(
        "coverage_is_reported_across_chunk_boundaries",
        |config: &mut super::RedisStoreConfig| {
            config.batch_chunk = 2;
        }
    );
    let mut ids = Vec::new();
    for index in 0..7 {
        let id = SandboxId::new();
        if index % 2 == 0 {
            store.add(running(id)).await.unwrap();
        }
        ids.push(id);
    }

    let rows = store.get_many(&ids).await.unwrap();
    assert_eq!(rows.entries.len(), 4);
    assert_eq!(rows.covered.len(), ids.len());
    assert!(rows.covers(&ids));
}

/// 🔴 A record that will not decode is a backend fault, not an absence. The
/// caller's answer to an absence is to delete things.
#[tokio::test]
async fn an_undecodable_record_is_an_error_not_a_missing_sandbox() {
    let store = store_or_skip!("an_undecodable_record_is_an_error_not_a_missing_sandbox");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let mut connection = raw(&store);
    let _: () = connection
        .set(store.inner().keys().record(&id), "{\"version\":999}")
        .await
        .unwrap();

    let error = store.get(&id).await.unwrap_err();
    assert!(matches!(error, StoreError::Backend { .. }), "{error:?}");
    let error = store.get_many(&[id]).await.unwrap_err();
    assert!(matches!(error, StoreError::Backend { .. }), "{error:?}");
}

// ---------------------------------------------------------------------------
// two replicas
// ---------------------------------------------------------------------------

/// The whole point of the exercise: a record written by one replica is visible
/// to another, with the same incarnation.
#[tokio::test]
async fn a_record_written_by_one_replica_is_read_by_another() {
    let store = store_or_skip!("a_record_written_by_one_replica_is_read_by_another");
    let other = sibling(&store).await;
    let id = SandboxId::new();
    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();

    let seen = other
        .get(&id)
        .await
        .unwrap()
        .expect("visible from the sibling");
    assert_eq!(seen.execution_id, metadata.execution_id);

    // 🔴 The control: a store on a different key namespace sees nothing, which
    // rules out the two "replicas" having shared something other than Redis.
    let elsewhere = super::harness::store_for(
        "a_record_written_by_one_replica_is_read_by_another",
        |config| {
            config.key_prefix = "agentenv:api-elsewhere".to_string();
        },
    )
    .await
    .unwrap();
    assert!(elsewhere.get(&id).await.unwrap().is_none());
}

/// Two replicas asking for opposite things: exactly one wins, and the loser is
/// told it lost rather than being allowed to proceed.
#[tokio::test]
async fn two_replicas_racing_opposite_transitions_produce_one_winner() {
    let store = store_or_skip!("two_replicas_racing_opposite_transitions_produce_one_winner");
    let other = sibling(&store).await;
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let pause = store.start_transition(
        &id,
        TransitionRequest::new(SandboxState::Pausing, vec![SandboxState::Running])
            .with_effect(TransitionEffect::Terminal(SandboxState::Paused)),
    );
    let kill = other.start_transition(
        &id,
        TransitionRequest::new(SandboxState::Killing, vec![SandboxState::Running])
            .with_effect(TransitionEffect::Removal),
    );
    let (pause, kill) = tokio::join!(pause, kill);

    let started = [&pause, &kill]
        .iter()
        .filter(|outcome| matches!(outcome, Ok(TransitionOutcome::Started(_))))
        .count();
    assert_eq!(
        started, 1,
        "exactly one of two opposite transitions may start: pause={pause:?} kill={kill:?}"
    );

    // The loser is refused, not silently allowed through.
    let loser_was_refused = matches!(
        (&pause, &kill),
        (Err(_), Ok(TransitionOutcome::Started(_)))
            | (Ok(TransitionOutcome::Started(_)), Err(_))
            | (
                Ok(TransitionOutcome::InFlight { .. }),
                Ok(TransitionOutcome::Started(_))
            )
            | (
                Ok(TransitionOutcome::Started(_)),
                Ok(TransitionOutcome::InFlight { .. })
            )
    );
    assert!(loser_was_refused, "pause={pause:?} kill={kill:?}");

    // And the sandbox is in one determinate state, not a superposition.
    let state = store.get(&id).await.unwrap().map(|record| record.state);
    assert!(
        matches!(
            state,
            Some(SandboxState::Pausing) | Some(SandboxState::Killing)
        ),
        "{state:?}"
    );
}

/// Concurrent closure updates from two replicas: both callbacks run at most
/// once each, and every accepted write is reflected. With the lock on, the
/// loser waits and succeeds; the assertion is that nothing is lost.
#[tokio::test]
async fn concurrent_closure_updates_from_two_replicas_do_not_lose_writes() {
    let store = store_or_skip!("concurrent_closure_updates_from_two_replicas_do_not_lose_writes");
    let other = sibling(&store).await;
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let before = stored_rev(&store, &id).await;

    let first = store.update_if_state(&id, &[SandboxState::Running], |metadata| {
        metadata.snapshot_id = "a".to_string();
    });
    let second = other.update_if_state(&id, &[SandboxState::Running], |metadata| {
        metadata.user_metadata = Some(std::collections::HashMap::from([(
            "b".to_string(),
            "b".to_string(),
        )]));
    });
    let (first, second) = tokio::join!(first, second);

    let winners = [first.is_ok(), second.is_ok()]
        .iter()
        .filter(|ok| **ok)
        .count();
    assert!(winners >= 1, "at least one update must land");
    let after = stored_rev(&store, &id).await;
    assert_eq!(
        after,
        before + winners as u64,
        "the revision must advance exactly once per accepted write"
    );
}

/// 🔴 The control that separates the lock from the predicates.
///
/// With the lock switched off, two replicas contending for the same record
/// still cannot corrupt it — the loser is *refused*, loudly, rather than
/// waiting its turn. This is why "switch the lock off and watch a double
/// execution appear" is not a valid probe for this design: the lock is the
/// throughput mechanism, and switching it off changes how often a race is lost,
/// not whether losing one costs anything.
#[tokio::test]
async fn with_the_lock_off_contention_is_refused_rather_than_corrupting() {
    let store = store_or_skip!(
        "with_the_lock_off_contention_is_refused_rather_than_corrupting",
        |config: &mut super::RedisStoreConfig| {
            config.distributed_lock_enabled = false;
        }
    );
    let other = sibling(&store).await;
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let before = stored_rev(&store, &id).await;

    let mut refusals = 0;
    let mut accepted = 0;
    for round in 0..12 {
        let first = store.update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.snapshot_id = format!("a{round}");
        });
        let second = other.update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.snapshot_id = format!("b{round}");
        });
        // 🔴 `join!`, not two sequential awaits. Awaiting one and then the
        // other produces no contention at all, and the assertion below would
        // then be measuring a race that never happened.
        let (first, second) = tokio::join!(first, second);
        for outcome in [first, second] {
            match outcome {
                Ok(_) => accepted += 1,
                Err(StoreError::ConcurrentUpdate { .. }) => refusals += 1,
                Err(other) => panic!("unexpected failure under contention: {other:?}"),
            }
        }
    }

    // 🔴 A counter that is zero proves nothing, so this asserts the race was
    // actually produced. If it were zero, the probe simply did not contend.
    assert!(
        refusals > 0,
        "no contention was produced, so this run says nothing about what happens under it"
    );
    // And every accepted write, and only those, advanced the record.
    assert_eq!(stored_rev(&store, &id).await, before + accepted as u64);
    let final_snapshot = store.get(&id).await.unwrap().unwrap().snapshot_id;
    assert!(
        final_snapshot.starts_with('a') || final_snapshot.starts_with('b'),
        "the record holds one writer's value, not a blend: {final_snapshot}"
    );
}

// ---------------------------------------------------------------------------
// the aggregate memo
// ---------------------------------------------------------------------------

/// The aggregate listing is a sample, and says when it was taken. The other
/// listing methods are not memoised, because a caller asking about particular
/// sandboxes is asking a different question.
#[tokio::test]
async fn the_aggregate_listing_is_a_dated_sample_and_the_others_are_not() {
    let store = store_or_skip!(
        "the_aggregate_listing_is_a_dated_sample_and_the_others_are_not",
        |config: &mut super::RedisStoreConfig| {
            config.metrics_memo_ttl = Duration::from_secs(60);
        }
    );
    let first = SandboxId::new();
    store.add(running(first)).await.unwrap();

    let mut count = 0;
    store.list_with_callback(|_| count += 1).await.unwrap();
    assert_eq!(count, 1);
    assert!(store.last_listing_sample_at().await.is_some());

    // A record added through a *different* replica does not invalidate this
    // replica's memo, which is exactly what makes the answer a sample.
    let other = sibling(&store).await;
    other.add(running(SandboxId::new())).await.unwrap();

    let mut count = 0;
    store.list_with_callback(|_| count += 1).await.unwrap();
    assert_eq!(count, 1, "the memo is still fresh");

    // But `list` is never memoised.
    assert_eq!(store.list().await.unwrap().len(), 2);
}
