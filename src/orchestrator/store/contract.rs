//! One set of assertions, run against every backend.
//!
//! # 🔴 What this is for
//!
//! `CLAUDE.md` records the scar in one sentence: *"a change made to the
//! in-memory store and forgotten for Redis is invisible everywhere else."* This
//! module is the direct answer. Each function below is an assertion about the
//! **shared** contract of [`MetadataStore`]; both backends run all of them, so
//! changing one and forgetting the other turns red.
//!
//! # 🔴 What it deliberately does not cover
//!
//! The four cluster primitives — `start_transition`, `reserve`,
//! `heal_expiry_index`, `reap_stuck_transitions` — are **not** here, and their
//! absence is not an oversight. They mean something on a store several replicas
//! share and nothing on a node's private ledger, so the two backends are not
//! equivalent on them and never should be. Asserting equivalence would either
//! force a meaningless imitation into the in-memory store or weaken the
//! assertion until it proved nothing. They are covered in the Redis suite
//! alone.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use super::{MetadataStore, PausedHandle, SandboxListFilter, SandboxMetadata, StoreError};
use crate::orchestrator::SandboxState;
use crate::types::SandboxId;

fn running(id: SandboxId) -> SandboxMetadata {
    SandboxMetadata {
        id,
        state: SandboxState::Running,
        ..Default::default()
    }
}

pub(crate) async fn add_get_remove_round_trip<S: MetadataStore>(store: &S) {
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
    // 🔴 A second removal is `None`, not an error: "there was nothing here" is
    // an answer, and callers rely on it being idempotent.
    assert!(store.remove(&id).await.unwrap().is_none());
    assert!(store.list_ids().await.unwrap().is_empty());
}

pub(crate) async fn add_refuses_a_duplicate<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();
    let error = store.add(running(id)).await.unwrap_err();
    assert!(
        matches!(error, StoreError::SandboxAlreadyExists { sandbox_id } if sandbox_id == id),
        "{error:?}"
    );
}

pub(crate) async fn missing_records_are_reported_as_missing<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    assert!(store.get(&id).await.unwrap().is_none());
    let error = store
        .update_state_if_state(&id, SandboxState::Paused, &[SandboxState::Running])
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

pub(crate) async fn state_cas_moves_only_from_an_expected_state<S: MetadataStore>(store: &S) {
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

pub(crate) async fn update_if_state_runs_the_callback_exactly_once<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    // 🔴 The callback writes a variable outside itself, exactly as `keep_alive`
    // does. If any backend ever turned this into a retry loop, this counter
    // would exceed one for a single logical update.
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

pub(crate) async fn update_if_state_refuses_the_wrong_state<S: MetadataStore>(store: &S) {
    let id = SandboxId::new();
    store.add(running(id)).await.unwrap();

    let mut called = false;
    let error = store
        .update_if_state(&id, &[SandboxState::Paused], |_| called = true)
        .await
        .unwrap_err();
    // 🔴 The callback must not run when the guard fails: a caller's callback
    // may have side effects, and running one for an update that is then refused
    // reports work that did not happen.
    assert!(!called);
    assert!(
        matches!(error, StoreError::StateConflict { .. }),
        "{error:?}"
    );
}

pub(crate) async fn update_writes_the_whole_record<S: MetadataStore>(store: &S) {
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

pub(crate) async fn filters_match_states_and_metadata<S: MetadataStore>(store: &S) {
    let running_id = SandboxId::new();
    let paused_id = SandboxId::new();

    let mut a = running(running_id);
    a.user_metadata = Some(HashMap::from([("env".to_string(), "prod".to_string())]));
    let mut b = running(paused_id);
    b.state = SandboxState::Paused;
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

    let not_paused = store
        .list_filtered(SandboxListFilter {
            excluded_states: Some(vec![SandboxState::Paused]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(not_paused.len(), 1);
    assert_eq!(not_paused[0].id, running_id);

    let dev = store
        .list_filtered(SandboxListFilter {
            user_metadata: Some(HashMap::from([("env".to_string(), "dev".to_string())])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dev.len(), 1);
    assert_eq!(dev[0].id, paused_id);

    // A record with no user metadata at all matches nothing that requires some.
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

pub(crate) async fn list_with_callback_visits_every_record<S: MetadataStore>(store: &S) {
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

pub(crate) async fn expiry_listing_is_bounded_and_ordered<S: MetadataStore>(store: &S) {
    let now = SystemTime::now();
    let mut ids = Vec::new();
    for offset in 1..=3u64 {
        let id = SandboxId::new();
        let mut metadata = running(id);
        // Already expired, by a widening margin.
        metadata.expires_at = Some(now - Duration::from_secs(offset * 10));
        store.add(metadata).await.unwrap();
        ids.push(id);
    }
    // One that is not due yet.
    let live = SandboxId::new();
    let mut metadata = running(live);
    metadata.expires_at = Some(now + Duration::from_secs(3600));
    store.add(metadata).await.unwrap();

    let all = store.list_expired(now).await.unwrap();
    assert_eq!(all.len(), 3, "the live sandbox must not be listed");
    assert!(all.iter().all(|record| record.id != live));

    // 🔴 The bound is the whole point of the batched form: an evictor running
    // on several replicas must do a fixed amount of work per round.
    let batch = store.expired_batch(now, 2).await.unwrap();
    assert_eq!(batch.len(), 2);
}

pub(crate) async fn get_many_reports_what_it_covered<S: MetadataStore>(store: &S) {
    let present = SandboxId::new();
    let absent = SandboxId::new();
    store.add(running(present)).await.unwrap();

    let rows = store.get_many(&[present, absent]).await.unwrap();
    assert!(rows.entries.contains_key(&present));
    assert!(!rows.entries.contains_key(&absent));
    // 🔴 `covered` lists both, present and absent alike. A caller deletes local
    // artifacts on the strength of an absence, so it has to be able to tell
    // "asked, and there is no record" from "never asked".
    assert_eq!(rows.covered.len(), 2);
    assert!(rows.covers(&[present, absent]));

    // 🔴 An empty batch asks nothing and covers nothing.
    let rows = store.get_many(&[]).await.unwrap();
    assert!(rows.entries.is_empty());
    assert!(rows.covered.is_empty());
    assert!(rows.covers(&[]));
}

pub(crate) async fn waiting_returns_immediately_when_not_transitional<S: MetadataStore>(store: &S) {
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

pub(crate) async fn waiting_on_a_missing_record_is_none<S: MetadataStore>(store: &S) {
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

pub(crate) async fn waiting_wakes_when_the_state_settles<S: MetadataStore + 'static>(store: &S) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.state = SandboxState::Pausing;
    store.add(metadata).await.unwrap();

    let waiting = store.wait_while_in_states(&id, &[SandboxState::Pausing]);
    let settling = async {
        tokio::time::sleep(Duration::from_millis(80)).await;
        store
            .update_state_if_state(&id, SandboxState::Paused, &[SandboxState::Pausing])
            .await
            .unwrap();
    };
    let (settled, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(waiting, settling)
    })
    .await
    .expect("waiter should have woken");
    assert_eq!(settled.unwrap().unwrap().state, SandboxState::Paused);
}

pub(crate) async fn waiting_returns_none_when_the_record_is_removed<S: MetadataStore>(store: &S) {
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

/// 🔴 The lifetime clock is reconciled by the store after every mutation, and
/// it is a shared contract rather than an in-memory implementation detail.
/// Getting it wrong on one backend means sandboxes that gain or lose budget
/// depending on which role wrote them.
pub(crate) async fn the_lifetime_clock_is_reconciled_after_every_write<S: MetadataStore>(
    store: &S,
) {
    let id = SandboxId::new();
    let mut metadata = running(id);
    metadata.max_lifetime = Some(Duration::from_secs(3600));
    metadata.running_since = None;
    store.add(metadata).await.unwrap();

    // Adding a running record starts the clock.
    let got = store.get(&id).await.unwrap().unwrap();
    assert!(
        got.running_since.is_some(),
        "a running record must have its clock started"
    );

    // Pausing charges the run and stops the clock.
    store
        .update_state_if_state(&id, SandboxState::Paused, &[SandboxState::Running])
        .await
        .unwrap();
    let paused = store.get(&id).await.unwrap().unwrap();
    assert!(
        paused.running_since.is_none(),
        "a paused record spends nothing"
    );

    // And a callback that sets the state directly is reconciled just the same.
    store
        .update_if_state(&id, &[SandboxState::Paused], |metadata| {
            metadata.state = SandboxState::Running;
        })
        .await
        .unwrap();
    let resumed = store.get(&id).await.unwrap().unwrap();
    assert!(resumed.running_since.is_some());
}

/// 🔴 The one thing both backends must agree on about paused state, even
/// though they answer it with different variants.
///
/// The two answers differ — an in-process store hands back the handle, a shared
/// store hands back a reference to the node that holds the bytes — but neither
/// may say `NotPaused` about a sandbox that is paused. That confusion is what
/// makes a resume fail with a message describing a state the sandbox is not in,
/// and it is the same shape as a read that conflated "not yet" with "never".
pub(crate) async fn a_paused_sandbox_never_answers_not_paused<S: MetadataStore>(store: &S) {
    #[derive(Debug)]
    struct FakePausedState;

    impl crate::sandbox::PausedSandboxState for FakePausedState {
        fn encode(&self) -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::json!({"fake": true}))
        }

        fn runtime_artifacts(&self) -> crate::sandbox::RuntimeArtifactSet {
            crate::sandbox::RuntimeArtifactSet::default()
        }
    }

    let paused = SandboxId::new();
    let mut metadata = running(paused);
    metadata.state = crate::orchestrator::SandboxState::Paused;
    metadata.paused_state = Some(std::sync::Arc::new(FakePausedState));
    store.add(metadata).await.unwrap();

    let handle = store.paused_handle(&paused).await.unwrap();
    assert!(
        !matches!(handle, PausedHandle::NotPaused),
        "a paused sandbox reported as having no paused state: {handle:?}"
    );

    // The control: a running sandbox really has none, and says so.
    let live = SandboxId::new();
    store.add(running(live)).await.unwrap();
    assert!(matches!(
        store.paused_handle(&live).await.unwrap(),
        PausedHandle::NotPaused
    ));

    // And a sandbox that is not there at all is neither of those.
    let error = store.paused_handle(&SandboxId::new()).await.unwrap_err();
    assert!(
        matches!(error, StoreError::SandboxNotFound { .. }),
        "{error:?}"
    );
}

/// Names every contract assertion, so a backend's suite is one line per test
/// and a new assertion cannot be added to one backend only.
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

/// The list itself, so both backends run the same one.
macro_rules! metadata_store_contract {
    () => {
        crate::orchestrator::store::contract::metadata_store_contract_suite!(
            add_get_remove_round_trip,
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
            a_paused_sandbox_never_answers_not_paused,
        );
    };
}

pub(crate) use {metadata_store_contract, metadata_store_contract_suite};
