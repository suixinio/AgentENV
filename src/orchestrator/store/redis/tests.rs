//! Redis metadata-store integration tests.

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

mod contract {
    use super::super::harness;
    use super::super::RedisMetadataStore;

    async fn new_contract_store(test: &str) -> Option<RedisMetadataStore> {
        harness::store_for(test, |_| {}).await
    }

    crate::orchestrator::store::contract::metadata_store_contract!();
}

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

    let read_back = store.get(&id).await.unwrap().unwrap();
    assert!(read_back.paused_state.is_none());
}

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

    assert!(store
        .get(&id)
        .await
        .unwrap()
        .unwrap()
        .paused_state
        .is_none());
}

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

    // Sleep past whole-second TTL rounding granularity.
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

#[tokio::test]
async fn an_uncapped_sandbox_has_no_record_ttl() {
    let store = store_or_skip!("an_uncapped_sandbox_has_no_record_ttl");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.max_lifetime = None;
    store.add(metadata).await.unwrap();
    assert_eq!(record_pttl(&store, &id).await, -1);

    store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();
    assert_eq!(record_pttl(&store, &id).await, -1);
}

#[tokio::test]
async fn a_paused_records_ttl_stops_shrinking_while_a_running_ones_does_not() {
    let store =
        store_or_skip!("a_paused_records_ttl_stops_shrinking_while_a_running_ones_does_not");

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

    let running_id = SandboxId::new();
    store
        .add(capped(running_id, Duration::from_secs(600)))
        .await
        .unwrap();
    let running_before = record_pttl(&store, &running_id).await;

    // Sleep past whole-second TTL rounding granularity.
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

#[tokio::test]
async fn a_write_for_a_superseded_incarnation_is_refused() {
    let store = store_or_skip!("a_write_for_a_superseded_incarnation_is_refused");
    let id = SandboxId::new();
    let original = running(id);
    store.add(original.clone()).await.unwrap();

    store.remove(&id).await.unwrap();
    let mut reborn = running(id);
    reborn.execution_id = ExecutionId::new();
    store.add(reborn.clone()).await.unwrap();

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

    let live = store.get(&id).await.unwrap().unwrap();
    assert_eq!(live.execution_id, reborn.execution_id);
    assert_ne!(live.snapshot_id, "written-by-the-dead-incarnation");
}

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

// Replays a stale write after a lockless replacement, bypassing Rust prechecks.
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
            // Intentionally blocks beyond the callback budget.
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

#[tokio::test]
async fn a_callback_within_its_budget_is_written() {
    let store = store_or_skip!(
        "a_callback_within_its_budget_is_written",
        |config: &mut super::RedisStoreConfig| {
            // Use the same callback duration while varying only its budget.
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

    store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();

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

#[tokio::test]
async fn removing_a_dead_incarnations_expiry_member_leaves_the_live_one_indexed() {
    let store =
        store_or_skip!("removing_a_dead_incarnations_expiry_member_leaves_the_live_one_indexed");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.set_timeout(Some(Duration::from_secs(3600)));
    let first_execution = metadata.execution_id;
    store.add(metadata).await.unwrap();

    let second_execution = ExecutionId::new();
    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.execution_id = second_execution;
        })
        .await
        .unwrap();

    let dead = ExpiryMember::new(id, first_execution).encode();
    let live = ExpiryMember::new(id, second_execution).encode();
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

    let mut connection = raw(&store);
    let _: i64 = connection
        .zrem(store.inner().keys().expiry(), &dead)
        .await
        .unwrap();

    assert!(expiry_members(&store).await.contains(&live));

    store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
        })
        .await
        .unwrap();
    let expired = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert!(expired.iter().any(|record| record.id == id));
}

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

#[tokio::test]
async fn a_record_that_is_no_longer_due_is_rescored_not_evicted() {
    let store = store_or_skip!("a_record_that_is_no_longer_due_is_rescored_not_evicted");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() + Duration::from_secs(3600));
    store.add(metadata.clone()).await.unwrap();

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

#[tokio::test]
async fn a_write_is_abandoned_when_its_lock_will_not_outlive_it() {
    let store = store_or_skip!(
        "a_write_is_abandoned_when_its_lock_will_not_outlive_it",
        |config: &mut super::RedisStoreConfig| {
            config.closure_budget = Duration::from_secs(5);
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

#[tokio::test]
async fn a_transition_whose_owner_vanished_is_not_reported_as_success() {
    let store = store_or_skip!("a_transition_whose_owner_vanished_is_not_reported_as_success");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let TransitionOutcome::Started(guard) = start_pause(&store, &id).await else {
        panic!("the transition must start");
    };
    let transition_id = guard.transition_id().to_string();
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
            TransitionRequest::new(SandboxState::Creating, vec![SandboxState::Running]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::InvalidTransition { .. }),
        "an edge that does not exist is not the same as arriving a step late: {error:?}"
    );
}

#[tokio::test]
async fn an_eviction_refuses_a_sandbox_that_was_kept_alive_in_the_meantime() {
    let store = store_or_skip!("an_eviction_refuses_a_sandbox_that_was_kept_alive_in_the_meantime");
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
    store.add(metadata).await.unwrap();

    let due = store.expired_batch(SystemTime::now(), 10).await.unwrap();
    assert_eq!(due.len(), 1);

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
    assert_eq!(reaped, vec![id]);

    let record = store.get(&id).await.unwrap().unwrap();
    assert_eq!(record.state, SandboxState::Pausing);
    assert!(
        record.expires_at.is_some(),
        "the record must now be findable"
    );
    assert!(!expiry_members(&store).await.is_empty());
    assert!(transition_members(&store).await.is_empty());
}

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

#[tokio::test]
async fn the_reaper_drops_members_for_dead_incarnations_and_missing_records() {
    let store =
        store_or_skip!("the_reaper_drops_members_for_dead_incarnations_and_missing_records");
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let mut connection = raw(&store);
    let key = store.inner().keys().transition_index();
    let dead = TransitionMember::new(id, ExecutionId::new(), Uuid::now_v7()).encode();
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
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Running
    );
}

#[tokio::test]
async fn a_reservation_moves_through_its_three_states() {
    let store = store_or_skip!("a_reservation_moves_through_its_three_states");
    let id = SandboxId::new();
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the first caller must get the window");
    };

    let other = sibling(&store).await;
    let Reservation::AlreadyPending(waiter) = other.reserve(&id).await.unwrap() else {
        panic!("the second caller must find the window open");
    };

    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();
    guard.finish(Ok(())).await.unwrap();

    let seen = tokio::time::timeout(Duration::from_secs(5), waiter.wait())
        .await
        .expect("the waiter should have woken")
        .unwrap();
    assert_eq!(seen.id, id);
    assert_eq!(seen.execution_id, metadata.execution_id);

    assert!(matches!(
        store.reserve(&id).await.unwrap(),
        Reservation::AlreadyInStorage
    ));
}

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

#[tokio::test]
async fn a_creation_window_is_visible_before_the_record_exists() {
    let store = store_or_skip!("a_creation_window_is_visible_before_the_record_exists");
    let id = SandboxId::new();
    let Reservation::Reserved(guard) = store.reserve(&id).await.unwrap() else {
        panic!("the window should open");
    };
    let other = sibling(&store).await;
    assert!(other.get(&id).await.unwrap().is_none());
    let rows = other.get_many(&[id]).await.unwrap();
    assert!(rows.entries.is_empty());
    assert!(rows.covers(&[id]), "the read did cover this id");

    assert!(matches!(
        other.reserve(&id).await.unwrap(),
        Reservation::AlreadyPending(_)
    ));
    guard.finish(Ok(())).await.unwrap();
}

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

    let state = store.get(&id).await.unwrap().map(|record| record.state);
    assert!(
        matches!(
            state,
            Some(SandboxState::Pausing) | Some(SandboxState::Killing)
        ),
        "{state:?}"
    );
}

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
        // Run concurrently so the probe actually contends.
        let (first, second) = tokio::join!(first, second);
        for outcome in [first, second] {
            match outcome {
                Ok(_) => accepted += 1,
                Err(StoreError::ConcurrentUpdate { .. }) => refusals += 1,
                Err(other) => panic!("unexpected failure under contention: {other:?}"),
            }
        }
    }

    assert!(
        refusals > 0,
        "no contention was produced, so this run says nothing about what happens under it"
    );
    assert_eq!(stored_rev(&store, &id).await, before + accepted as u64);
    let final_snapshot = store.get(&id).await.unwrap().unwrap().snapshot_id;
    assert!(
        final_snapshot.starts_with('a') || final_snapshot.starts_with('b'),
        "the record holds one writer's value, not a blend: {final_snapshot}"
    );
}

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

    let other = sibling(&store).await;
    other.add(running(SandboxId::new())).await.unwrap();

    let mut count = 0;
    store.list_with_callback(|_| count += 1).await.unwrap();
    assert_eq!(count, 1, "the memo is still fresh");

    assert_eq!(store.list().await.unwrap().len(), 2);
}
