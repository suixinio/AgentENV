//! Shared assertions run against both binding-store backends.
//! Any backend change must pass this same suite.

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

pub async fn record_with_an_empty_sandbox_id_is_a_noop_that_reports_not_arbitrated<
    S: BindingStore,
>(
    store: &S,
) {
    for sandbox_id in ["", "   "] {
        let decision = store
            .record(
                sandbox_id,
                Binding {
                    node: node("node-a"),
                    execution_id: "exec-1".to_string(),
                    projection_ttl: Duration::ZERO,
                },
                unix(0),
            )
            .await
            .unwrap();
        assert_eq!(
            decision,
            BindingDecision::NotArbitrated,
            "a write naming no sandbox ({sandbox_id:?}) arbitrated something"
        );
        assert_eq!(
            decision.as_str(),
            "",
            "NotArbitrated's metric label must stay the empty string"
        );
        assert!(
            store.get(sandbox_id, unix(0)).await.unwrap().is_none(),
            "a write naming no sandbox ({sandbox_id:?}) still stored a record"
        );
    }

    let decision = store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::Installed);
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
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
            record_with_an_empty_sandbox_id_is_a_noop_that_reports_not_arbitrated,
        );
    };
}

pub(crate) use {binding_store_contract, binding_store_contract_suite};
