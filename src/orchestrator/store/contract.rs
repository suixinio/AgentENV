//! Shared metadata-store assertions run against both backends.
//! Any backend change must pass this same suite; cluster-only primitives remain
//! covered by Redis-specific tests.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use super::{FencedRemoval, MetadataStore, SandboxListFilter, SandboxMetadata, StoreError};
use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

fn running(id: SandboxId) -> SandboxMetadata {
    SandboxMetadata {
        id,
        state: SandboxState::Running,
        ..Default::default()
    }
}

pub async fn add_get_remove_round_trip<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.snapshot_id = "snap-1".to_string();
    metadata.set_timeout(Some(Duration::from_secs(600)));

    store.add(metadata.clone()).await.unwrap();

    let got = store.get(&id).await.unwrap().expect("record should exist");
    assert_eq!(got.id, id);
    assert_eq!(got.snapshot_id, "snap-1");
    assert_eq!(got.execution_id, metadata.execution_id);
    assert_eq!(got.state, SandboxState::Running);
    assert_eq!(got.timeout, Some(Duration::from_secs(600)));
    assert!(got.expires_at.is_some());

    assert_eq!(store.list_ids().await.unwrap(), vec![id]);
    assert_eq!(store.list().await.unwrap().len(), 1);

    let removed = store.remove(&id).await.unwrap().expect("remove returns it");
    assert_eq!(removed.id, id);
    assert!(store.get(&id).await.unwrap().is_none());
    assert!(store.remove(&id).await.unwrap().is_none());
    assert!(store.list_ids().await.unwrap().is_empty());
}

pub async fn a_fenced_removal_takes_back_only_its_own_record<S: MetadataStore>(store: &S) {
    let mine = ExecutionId::new();
    let theirs = ExecutionId::new();

    let creating = |id, execution_id| SandboxMetadata {
        id,
        execution_id,
        state: SandboxState::Creating,
        ..Default::default()
    };
    let own = SandboxId::new();
    let other_incarnation = SandboxId::new();
    let other_state = SandboxId::new();
    let absent = SandboxId::new();

    store.add(creating(own, mine)).await.unwrap();
    store
        .add(creating(other_incarnation, theirs))
        .await
        .unwrap();
    let mut moved_on = creating(other_state, mine);
    moved_on.state = SandboxState::Running;
    store.add(moved_on).await.unwrap();

    assert_eq!(
        store
            .remove_if_execution(&own, mine, &[SandboxState::Creating])
            .await
            .unwrap(),
        FencedRemoval::Removed
    );
    assert_eq!(
        store
            .remove_if_execution(&other_incarnation, mine, &[SandboxState::Creating])
            .await
            .unwrap(),
        FencedRemoval::Superseded {
            state: SandboxState::Creating,
            execution_id: theirs,
        }
    );
    assert_eq!(
        store
            .remove_if_execution(&other_state, mine, &[SandboxState::Creating])
            .await
            .unwrap(),
        FencedRemoval::Superseded {
            state: SandboxState::Running,
            execution_id: mine,
        }
    );
    assert_eq!(
        store
            .remove_if_execution(&absent, mine, &[SandboxState::Creating])
            .await
            .unwrap(),
        FencedRemoval::Absent
    );

    assert!(store.get(&own).await.unwrap().is_none());
    assert_eq!(
        store.get(&other_incarnation).await.unwrap().unwrap().state,
        SandboxState::Creating
    );
    assert_eq!(
        store.get(&other_state).await.unwrap().unwrap().state,
        SandboxState::Running
    );
    let ids = store.list_ids().await.unwrap();
    assert_eq!(ids.len(), 2, "{ids:?}");
    assert!(ids.contains(&other_incarnation), "{ids:?}");
    assert!(ids.contains(&other_state), "{ids:?}");
    assert!(!ids.contains(&own), "{ids:?}");

    assert_eq!(
        store
            .remove_if_execution(&own, mine, &[SandboxState::Creating])
            .await
            .unwrap(),
        FencedRemoval::Absent
    );
}

pub async fn add_refuses_a_duplicate<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let error = store.add(running(id)).await.unwrap_err();
    assert!(
        matches!(error, StoreError::SandboxAlreadyExists { sandbox_id } if sandbox_id == id),
        "{error:?}"
    );
}

pub async fn missing_records_are_reported_as_missing<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    assert!(store.get(&id).await.unwrap().is_none());
    let error = store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::SandboxNotFound { .. }),
        "{error:?}"
    );

    let error = store
        .update_if_state(&id, &[SandboxState::Running], |_| {})
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::SandboxNotFound { .. }),
        "{error:?}"
    );
}

pub async fn state_cas_moves_only_from_an_expected_state<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let previous = store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();
    assert_eq!(previous, SandboxState::Running);
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        SandboxState::Pausing
    );

    let error = store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap_err();
    match error {
        StoreError::StateConflict { actual_state, .. } => {
            assert_eq!(actual_state, SandboxState::Pausing);
        }
        other => panic!("expected a state conflict, got {other:?}"),
    }
}

pub async fn update_if_state_runs_the_callback_exactly_once<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let mut calls = 0u32;
    let result = store
        .update_if_state(&id, &[SandboxState::Running], |metadata| {
            calls += 1;
            metadata.state = SandboxState::Pausing;
            metadata.snapshot_id = "after".to_string();
        })
        .await
        .unwrap();

    assert_eq!(calls, 1);
    assert_eq!(result.previous.state, SandboxState::Running);
    assert_eq!(result.current.state, SandboxState::Pausing);
    assert_eq!(result.current.snapshot_id, "after");
    assert_eq!(store.get(&id).await.unwrap().unwrap().snapshot_id, "after");
}

pub async fn update_if_state_refuses_the_wrong_state<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let mut called = false;
    let error = store
        .update_if_state(&id, &[SandboxState::Pausing], |_| called = true)
        .await
        .unwrap_err();
    assert!(!called);
    assert!(
        matches!(error, StoreError::StateConflict { .. }),
        "{error:?}"
    );
}

pub async fn update_writes_the_whole_record<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    let metadata = running(id);
    store.add(metadata.clone()).await.unwrap();

    let mut next = metadata.clone();
    next.snapshot_id = "rewritten".to_string();
    next.user_metadata = Some(HashMap::from([("k".to_string(), "v".to_string())]));
    store.update(next).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    assert_eq!(got.snapshot_id, "rewritten");
    assert_eq!(
        got.user_metadata.unwrap().get("k").map(String::as_str),
        Some("v")
    );
}

pub async fn filters_match_states_and_metadata<S: MetadataStore>(store: &S) {
    let running_id = SandboxId::new();
    let pausing_id = SandboxId::new();

    let mut a = running(running_id);
    a.user_metadata = Some(HashMap::from([("env".to_string(), "prod".to_string())]));
    let mut b = running(pausing_id);
    b.state = SandboxState::Pausing;
    b.user_metadata = Some(HashMap::from([("env".to_string(), "dev".to_string())]));

    store.add(a).await.unwrap();
    store.add(b).await.unwrap();

    let only_running = store
        .list_filtered(SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(only_running.len(), 1);
    assert_eq!(only_running[0].id, running_id);

    let not_pausing = store
        .list_filtered(SandboxListFilter {
            excluded_states: Some(vec![SandboxState::Pausing]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(not_pausing.len(), 1);
    assert_eq!(not_pausing[0].id, running_id);

    let dev = store
        .list_filtered(SandboxListFilter {
            user_metadata: Some(HashMap::from([("env".to_string(), "dev".to_string())])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dev.len(), 1);
    assert_eq!(dev[0].id, pausing_id);

    let plain = SandboxId::new();
    store.add(running(plain)).await.unwrap();
    let dev = store
        .list_filtered(SandboxListFilter {
            user_metadata: Some(HashMap::from([("env".to_string(), "dev".to_string())])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dev.len(), 1);
}

pub async fn list_with_callback_visits_every_record<S: MetadataStore>(store: &S) {
    let ids: Vec<_> = (0..3).map(|_| SandboxId::new()).collect();
    for id in &ids {
        store.add(running(*id)).await.unwrap();
    }
    let mut seen = Vec::new();
    store
        .list_with_callback(|metadata| seen.push(metadata.id))
        .await
        .unwrap();
    seen.sort();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(seen, expected);
}

pub async fn expiry_listing_is_bounded_and_ordered<S: MetadataStore>(store: &S) {
    let now = SystemTime::now();
    let mut ids = Vec::new();
    for offset in 1..=3u64 {
        let id = SandboxId::new();
        let mut metadata = running(id);
        metadata.expires_at = Some(now - Duration::from_secs(offset * 10));
        store.add(metadata).await.unwrap();
        ids.push(id);
    }
    let live = SandboxId::new();
    let mut metadata = running(live);
    metadata.expires_at = Some(now + Duration::from_secs(3600));
    store.add(metadata).await.unwrap();

    let all = store.list_expired(now).await.unwrap();
    assert_eq!(all.len(), 3, "the live sandbox must not be listed");
    assert!(all.iter().all(|record| record.id != live));

    let batch = store.expired_batch(now, 2).await.unwrap();
    assert_eq!(batch.len(), 2);
}

pub async fn get_many_reports_what_it_covered<S: MetadataStore>(store: &S) {
    let present = SandboxId::new();
    let absent = SandboxId::new();
    store.add(running(present)).await.unwrap();

    let rows = store.get_many(&[present, absent]).await.unwrap();
    assert!(rows.entries.contains_key(&present));
    assert!(!rows.entries.contains_key(&absent));
    assert_eq!(rows.covered.len(), 2);
    assert!(rows.covers(&[present, absent]));

    let rows = store.get_many(&[]).await.unwrap();
    assert!(rows.entries.is_empty());
    assert!(rows.covered.is_empty());
    assert!(rows.covers(&[]));
}

pub async fn waiting_returns_immediately_when_not_transitional<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let settled = tokio::time::timeout(
        Duration::from_secs(2),
        store.wait_while_in_states(&id, &[SandboxState::Pausing]),
    )
    .await
    .expect("a non-transitional record must not block")
    .unwrap()
    .expect("record should still exist");
    assert_eq!(settled.state, SandboxState::Running);
}

pub async fn waiting_on_a_missing_record_is_none<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    let settled = tokio::time::timeout(
        Duration::from_secs(2),
        store.wait_while_in_states(&id, &[SandboxState::Pausing]),
    )
    .await
    .expect("a missing record must not block")
    .unwrap();
    assert!(settled.is_none());
}

pub async fn waiting_wakes_when_the_state_settles<S: MetadataStore + 'static>(store: &S) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Pausing;
    store.add(metadata).await.unwrap();

    let waiting = store.wait_while_in_states(&id, &[SandboxState::Pausing]);
    let settling = async {
        tokio::time::sleep(Duration::from_millis(80)).await;
        store
            .update_state_if_state(&id, SandboxState::Running, &[SandboxState::Pausing])
            .await
            .unwrap();
    };
    let (settled, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(waiting, settling)
    })
    .await
    .expect("waiter should have woken");
    assert_eq!(settled.unwrap().unwrap().state, SandboxState::Running);
}

pub async fn waiting_returns_none_when_the_record_is_removed<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Pausing;
    store.add(metadata).await.unwrap();

    let waiting = store.wait_while_in_states(&id, &[SandboxState::Pausing]);
    let removing = async {
        tokio::time::sleep(Duration::from_millis(80)).await;
        store.remove(&id).await.unwrap();
    };
    let (settled, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(waiting, removing)
    })
    .await
    .expect("waiter should have woken");
    assert!(settled.unwrap().is_none());
}

pub async fn the_lifetime_clock_is_reconciled_after_every_write<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.max_lifetime = Some(Duration::from_secs(3600));
    metadata.running_since = None;
    store.add(metadata).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    let opened = got
        .running_since
        .expect("a running record must have its clock started");

    store
        .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
        .await
        .unwrap();
    let pausing = store.get(&id).await.unwrap().unwrap();
    assert_eq!(
        pausing.running_since,
        Some(opened),
        "every recorded state spends the same run"
    );

    let mut dropped_clock = pausing.clone();
    dropped_clock.running_since = None;
    store.update(dropped_clock).await.unwrap();
    let written = store.get(&id).await.unwrap().unwrap();
    assert!(
        written.running_since.is_some(),
        "a write that lost the clock has it reopened"
    );
}

macro_rules! metadata_store_contract_suite {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = new_contract_store(stringify!($name)).await else {
                    return;
                };
                crate::orchestrator::store::contract::$name(&store).await;
            }
        )*
    };
}

macro_rules! metadata_store_contract {
    () => {
        crate::orchestrator::store::contract::metadata_store_contract_suite!(
            add_get_remove_round_trip,
            a_fenced_removal_takes_back_only_its_own_record,
            add_refuses_a_duplicate,
            missing_records_are_reported_as_missing,
            state_cas_moves_only_from_an_expected_state,
            update_if_state_runs_the_callback_exactly_once,
            update_if_state_refuses_the_wrong_state,
            update_writes_the_whole_record,
            filters_match_states_and_metadata,
            list_with_callback_visits_every_record,
            expiry_listing_is_bounded_and_ordered,
            get_many_reports_what_it_covered,
            waiting_returns_immediately_when_not_transitional,
            waiting_on_a_missing_record_is_none,
            waiting_wakes_when_the_state_settles,
            waiting_returns_none_when_the_record_is_removed,
            the_lifetime_clock_is_reconciled_after_every_write,
        );
    };
}

pub(crate) use {metadata_store_contract, metadata_store_contract_suite};
