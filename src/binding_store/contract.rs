//! Task's own "D1"/"D3": one set of assertions, run against both
//! [`super::InMemoryBindingStore`] and [`super::RedisBindingStore`] —
//! directly answering the gap the task called out in Go's own test suite:
//! *"the Redis backend was tested, but only at the store layer, not the
//! RPC layer — every `Service` in `projection_service_test.go` used
//! `NewInMemoryBindingStore`."* This module is the store-layer half (run
//! by both `in_memory::tests`/`redis::tests` via the macro below);
//! `src/node_registry/grpc_service.rs`'s `report_sandbox_event` tests are
//! the RPC-layer half that closes the actual gap, parametrized the same
//! way over both backends.
//!
//! Mirrors `src/orchestrator/store/contract.rs`'s own macro pattern
//! exactly, per CLAUDE.md's own scar: *"a change made to the in-memory
//! store and forgotten for Redis is invisible everywhere else."*

use std::time::{Duration, SystemTime};

use super::{Binding, BindingDecision, BindingDeleteOutcome, BindingStore};
use crate::node_registry::types::{Node, RosterEntry};

fn unix(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

fn node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        endpoint: format!("http://{id}:8000"),
        pod_name: String::new(),
    }
}

pub async fn get_is_none_for_an_absent_sandbox<S: BindingStore>(store: &S) {
    assert!(store.get("never-bound", unix(0)).await.unwrap().is_none());
}

pub async fn record_then_get_round_trips<S: BindingStore>(store: &S) {
    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "0198f5c0-1234-7abc-8def-000000000001".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::Installed);

    let binding = store.get("sbx-1", unix(0)).await.unwrap().expect("bound");
    assert_eq!(binding.node.id, "node-a");
    assert_eq!(binding.execution_id, "0198f5c0-1234-7abc-8def-000000000001");
}

pub async fn record_with_no_execution_id_installs_unknown<S: BindingStore>(store: &S) {
    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::InstalledUnknown);
}

/// Fenced arbitration (both backends default to it): a lexicographically
/// older challenger is refused and the existing record is left untouched.
pub async fn record_rejects_an_older_incarnation<S: BindingStore>(store: &S) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "b-newer".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();

    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-b"),
                execution_id: "a-older".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(1),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::RejectedOlder);

    let binding = store
        .get("sbx-1", unix(1))
        .await
        .unwrap()
        .expect("unchanged");
    assert_eq!(
        binding.node.id, "node-a",
        "a rejected write must not move the node"
    );
}

/// A newer incarnation is accepted and takes over.
pub async fn record_accepts_a_newer_incarnation<S: BindingStore>(store: &S) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "a-older".to_string(),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();
    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-b"),
                execution_id: "b-newer".to_string(),
                ..Default::default()
            },
            unix(1),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::Superseded);
    let binding = store.get("sbx-1", unix(1)).await.unwrap().expect("moved");
    assert_eq!(binding.node.id, "node-b");
}

pub async fn reconcile_node_installs_every_roster_entry<S: BindingStore>(store: &S) {
    let decisions = store
        .reconcile_node(
            node("node-a"),
            vec![
                RosterEntry {
                    sandbox_id: "sbx-1".to_string(),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                    paused: false,
                },
                RosterEntry {
                    sandbox_id: "sbx-2".to_string(),
                    execution_id: String::new(),
                    projection_ttl: Duration::ZERO,
                    paused: false,
                },
            ],
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decisions.len(), 2);
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
    assert!(store.get("sbx-2", unix(0)).await.unwrap().is_some());
}

pub async fn reconcile_node_with_an_empty_roster_removes_everything_it_owns<S: BindingStore>(
    store: &S,
) {
    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::ZERO,
                paused: false,
            }],
            unix(0),
        )
        .await
        .unwrap();
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());

    let decisions = store
        .reconcile_node(node("node-a"), vec![], unix(1))
        .await
        .unwrap();
    assert!(decisions.is_empty());
    assert!(store.get("sbx-1", unix(1)).await.unwrap().is_none());
}

pub async fn reconcile_node_drops_entries_the_node_no_longer_reports<S: BindingStore>(store: &S) {
    store
        .reconcile_node(
            node("node-a"),
            vec![
                RosterEntry {
                    sandbox_id: "sbx-1".to_string(),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                    paused: false,
                },
                RosterEntry {
                    sandbox_id: "sbx-2".to_string(),
                    execution_id: "exec-2".to_string(),
                    projection_ttl: Duration::ZERO,
                    paused: false,
                },
            ],
            unix(0),
        )
        .await
        .unwrap();

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::ZERO,
                paused: false,
            }],
            unix(1),
        )
        .await
        .unwrap();

    assert!(store.get("sbx-1", unix(1)).await.unwrap().is_some());
    assert!(
        store.get("sbx-2", unix(1)).await.unwrap().is_none(),
        "a sandbox dropped from the roster must be removed"
    );
}

pub async fn reconcile_node_does_not_touch_another_nodes_binding<S: BindingStore>(store: &S) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-b"),
                execution_id: "exec-1".to_string(),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();

    // node-a reconciling an empty roster must not delete sbx-1: it belongs
    // to node-b, not node-a.
    store
        .reconcile_node(node("node-a"), vec![], unix(1))
        .await
        .unwrap();
    let binding = store
        .get("sbx-1", unix(1))
        .await
        .unwrap()
        .expect("still bound to node-b");
    assert_eq!(binding.node.id, "node-b");
}

// ---- delete: "the guard is the whole point" ----

pub async fn delete_of_an_absent_sandbox_is_a_noop<S: BindingStore>(store: &S) {
    let outcome = store
        .delete("never-bound", "exec-1", unix(0))
        .await
        .unwrap();
    assert_eq!(outcome, BindingDeleteOutcome::Absent);
}

pub async fn delete_with_the_matching_incarnation_deletes<S: BindingStore>(store: &S) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "exec-1".to_string(),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();
    let outcome = store.delete("sbx-1", "exec-1", unix(0)).await.unwrap();
    assert_eq!(outcome, BindingDeleteOutcome::Deleted);
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_none());
}

pub async fn delete_with_a_stale_incarnation_is_refused_and_the_record_survives<S: BindingStore>(
    store: &S,
) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "exec-2".to_string(),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();

    // A late PAUSE event for exec-1, but the sandbox has since been resumed
    // elsewhere under exec-2. This is the actual bug this guard exists to
    // prevent: without it, a late event would tear down the live record.
    let outcome = store.delete("sbx-1", "exec-1", unix(1)).await.unwrap();
    assert_eq!(outcome, BindingDeleteOutcome::RejectedStale);

    let binding = store
        .get("sbx-1", unix(1))
        .await
        .unwrap()
        .expect("must survive the refused delete");
    assert_eq!(
        binding.execution_id, "exec-2",
        "the live incarnation must be untouched"
    );
}

pub async fn delete_of_a_record_with_no_known_incarnation_deletes_anyway<S: BindingStore>(
    store: &S,
) {
    // A record that never named an incarnation (arbitration-off, or an old
    // writer) -- an event carrying a known execution id is stronger
    // evidence than a record that never named one, so this deletes rather
    // than refusing -- deliberately asymmetric with the write path.
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();
    let outcome = store.delete("sbx-1", "exec-1", unix(0)).await.unwrap();
    assert_eq!(outcome, BindingDeleteOutcome::DeletedUnknownIncumbent);
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_none());
}

pub async fn delete_with_an_empty_execution_id_is_a_noop_never_an_unguarded_delete<
    S: BindingStore,
>(
    store: &S,
) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "exec-1".to_string(),
                ..Default::default()
            },
            unix(0),
        )
        .await
        .unwrap();
    let outcome = store.delete("sbx-1", "", unix(0)).await.unwrap();
    assert_eq!(outcome, BindingDeleteOutcome::Absent);
    assert!(
        store.get("sbx-1", unix(0)).await.unwrap().is_some(),
        "an empty execution id must never delete a live record"
    );
}

// ---- Arbitration-mode contract: Observing/Off, run against both backends ----
//
// Every function above runs under `ArbitrationMode::Fenced` only (both
// backends' `new_contract_store` build with `BindingStoreSettings::default()`,
// whose `arbitration` field defaults to `Fenced`). `arbitration.rs`'s own
// unit tests (`observing_always_accepts_but_reports_the_same_decision_fenced_would`,
// `off_always_accepts_and_reports_nothing`) already prove the pure
// `arbitrate_observing`/`arbitrate_off` functions are correct in isolation —
// what neither backend's store-level suite ever exercised is that
// `BindingStore::record`/`reconcile_node` actually *thread* the configured
// mode through to those functions rather than hard-wiring `Fenced`'s
// accept/reject behavior regardless of what `BindingStoreSettings` said.
// A backend that ignored `settings.arbitration` entirely would still pass
// every test above (they never construct a non-Fenced store) and would
// still pass `arbitration.rs`'s pure unit tests (they never touch a real
// `BindingStore` at all) — these two close exactly that gap, for both
// backends, the same "one suite, run twice" discipline the rest of this
// file already uses.

/// Observing: the decision label still reads what `Fenced` would have
/// decided (so the metric a caller watches to judge "would enforcing this
/// break anything" is honest), but the write always lands regardless.
pub async fn record_observing_mode_accepts_but_still_labels_the_fenced_decision<S: BindingStore>(
    store: &S,
) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "b-newer".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();

    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-b"),
                execution_id: "a-older".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(1),
        )
        .await
        .unwrap();
    assert_eq!(
        decision,
        BindingDecision::RejectedOlder,
        "the decision label must still read what Fenced would have decided"
    );

    let binding = store.get("sbx-1", unix(1)).await.unwrap().expect("bound");
    assert_eq!(
        binding.node.id, "node-b",
        "Observing must move the record even though the decision reads RejectedOlder -- a \
         backend that only checked the decision, not the separate accept bool, would leave \
         this at node-a and still pass every Fenced-mode assertion above"
    );
    assert_eq!(binding.execution_id, "a-older");
}

/// Off: always accepts, and reports no decision at all -- nothing for
/// `agentenv_api_binding_execution_total` to count.
pub async fn record_off_mode_accepts_anything_and_reports_no_decision<S: BindingStore>(store: &S) {
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "b-newer".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();

    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-b"),
                execution_id: "a-older".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(1),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::NotArbitrated);

    let binding = store.get("sbx-1", unix(1)).await.unwrap().expect("bound");
    assert_eq!(
        binding.node.id, "node-b",
        "Off must move the record unconditionally"
    );
}

macro_rules! binding_store_arbitration_contract_suite {
    ($($name:ident: $mode:expr),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = new_contract_store_with_mode(stringify!($name), $mode).await else {
                    return;
                };
                crate::binding_store::contract::$name(&store).await;
            }
        )*
    };
}

/// Companion to [`binding_store_contract`]: each backend's `mod contract`
/// calls this too, alongside a `new_contract_store_with_mode(test, mode)`
/// (the same shape as `new_contract_store`, plus the mode to build under).
macro_rules! binding_store_arbitration_contract {
    () => {
        crate::binding_store::contract::binding_store_arbitration_contract_suite!(
            record_observing_mode_accepts_but_still_labels_the_fenced_decision:
                crate::binding_store::ArbitrationMode::Observing,
            record_off_mode_accepts_anything_and_reports_no_decision:
                crate::binding_store::ArbitrationMode::Off,
        );
    };
}

macro_rules! binding_store_contract_suite {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = new_contract_store(stringify!($name)).await else {
                    return;
                };
                crate::binding_store::contract::$name(&store).await;
            }
        )*
    };
}

macro_rules! binding_store_contract {
    () => {
        crate::binding_store::contract::binding_store_contract_suite!(
            get_is_none_for_an_absent_sandbox,
            record_then_get_round_trips,
            record_with_no_execution_id_installs_unknown,
            record_rejects_an_older_incarnation,
            record_accepts_a_newer_incarnation,
            reconcile_node_installs_every_roster_entry,
            reconcile_node_with_an_empty_roster_removes_everything_it_owns,
            reconcile_node_drops_entries_the_node_no_longer_reports,
            reconcile_node_does_not_touch_another_nodes_binding,
            delete_of_an_absent_sandbox_is_a_noop,
            delete_with_the_matching_incarnation_deletes,
            delete_with_a_stale_incarnation_is_refused_and_the_record_survives,
            delete_of_a_record_with_no_known_incarnation_deletes_anyway,
            delete_with_an_empty_execution_id_is_a_noop_never_an_unguarded_delete,
        );
    };
}

pub(crate) use {
    binding_store_arbitration_contract, binding_store_arbitration_contract_suite,
    binding_store_contract, binding_store_contract_suite,
};
