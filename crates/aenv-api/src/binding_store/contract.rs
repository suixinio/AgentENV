//! Shared assertions run against both binding-store backends.
//! Any backend change must pass this same suite.

use std::time::{Duration, SystemTime};

use super::reservation::LaunchReservationOutcome;
use super::{Binding, BindingDecision, BindingDeleteOutcome, BindingState, BindingStore};
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
                state: BindingState::Confirmed,
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
                state: BindingState::Confirmed,
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
                state: BindingState::Confirmed,
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
                },
                RosterEntry {
                    sandbox_id: "sbx-2".to_string(),
                    execution_id: String::new(),
                    projection_ttl: Duration::ZERO,
                },
            ],
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decisions.len(), 2);
    // A roster reaches routing only through these bindings, so each one has to
    // carry the whole answer a lookup needs.
    let known = store
        .get("sbx-1", unix(0))
        .await
        .unwrap()
        .expect("the roster named sbx-1");
    assert_eq!(known.node.id, "node-a");
    assert_eq!(known.execution_id, "exec-1");
    assert_eq!(known.state, BindingState::Confirmed);
    let unknown = store
        .get("sbx-2", unix(0))
        .await
        .unwrap()
        .expect("an entry with no incarnation still names its node");
    assert_eq!(unknown.node.id, "node-a");
    assert!(unknown.execution_id.is_empty());
    assert_eq!(unknown.state, BindingState::Confirmed);
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
                },
                RosterEntry {
                    sandbox_id: "sbx-2".to_string(),
                    execution_id: "exec-2".to_string(),
                    projection_ttl: Duration::ZERO,
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
                    state: BindingState::Confirmed,
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
                state: BindingState::Confirmed,
            },
            unix(0),
        )
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::Installed);
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
}

fn reservation(node_id: &str, execution_id: &str) -> Binding {
    Binding {
        node: node(node_id),
        execution_id: execution_id.to_string(),
        projection_ttl: Duration::from_secs(300),
        state: BindingState::Starting,
    }
}

fn confirmation(node_id: &str, execution_id: &str) -> Binding {
    Binding {
        node: node(node_id),
        execution_id: execution_id.to_string(),
        projection_ttl: Duration::ZERO,
        state: BindingState::Confirmed,
    }
}

fn roster(sandbox_id: &str, execution_id: &str) -> RosterEntry {
    RosterEntry {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        projection_ttl: Duration::ZERO,
    }
}

pub async fn a_reservation_reads_back_as_starting<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    let binding = store
        .get("sbx-1", unix(0))
        .await
        .unwrap()
        .expect("reserved");
    assert_eq!(binding.state, BindingState::Starting);
    assert_eq!(binding.node.id, "node-a");
}

pub async fn a_confirmation_over_a_reservation_promotes_it<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    let decision = store
        .record("sbx-1", confirmation("node-a", "exec-1"), unix(1))
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::Refreshed);
    assert_eq!(
        store
            .get("sbx-1", unix(1))
            .await
            .unwrap()
            .expect("still bound")
            .state,
        BindingState::Confirmed,
        "the launch's own confirmation left the record saying a create is still in flight"
    );
}

pub async fn a_heartbeat_that_has_not_heard_of_a_reservation_leaves_it_alone<S: BindingStore>(
    store: &S,
) {
    store
        .record("reserved", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    store
        .record("running", confirmation("node-a", "exec-2"), unix(0))
        .await
        .unwrap();

    // The node is running one sandbox and has not acknowledged the create yet.
    store
        .reconcile_node(node("node-a"), vec![roster("running", "exec-2")], unix(1))
        .await
        .unwrap();

    assert_eq!(
        store
            .get("reserved", unix(1))
            .await
            .unwrap()
            .expect("a heartbeat retired a create that had not finished")
            .state,
        BindingState::Starting
    );
    assert!(store.get("running", unix(1)).await.unwrap().is_some());
}

pub async fn an_empty_roster_still_leaves_a_reservation_alone<S: BindingStore>(store: &S) {
    store
        .record("reserved", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    store
        .reconcile_node(node("node-a"), Vec::new(), unix(1))
        .await
        .unwrap();
    assert!(
        store.get("reserved", unix(1)).await.unwrap().is_some(),
        "a node that reported nothing retired a create that had not finished"
    );
}

pub async fn a_heartbeat_that_names_a_reservation_confirms_it<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    store
        .reconcile_node(node("node-a"), vec![roster("sbx-1", "exec-1")], unix(1))
        .await
        .unwrap();
    assert_eq!(
        store
            .get("sbx-1", unix(1))
            .await
            .unwrap()
            .expect("still bound")
            .state,
        BindingState::Confirmed,
        "the node reported it running and the record still says it is starting"
    );
}

/// A reservation is exclusive for [`super::LAUNCH_RESERVATION_EXCLUSIVE_TTL`],
/// which these tests straddle: `unix(60)` is inside it and `unix(200)` is past
/// it, while the reservation's own 300s entry TTL keeps the record present for
/// both.
pub async fn a_newer_launch_does_not_supersede_a_reservation_still_in_flight<S: BindingStore>(
    store: &S,
) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();

    let decision = store
        .record("sbx-1", reservation("node-b", "exec-2"), unix(60))
        .await
        .unwrap();
    assert_eq!(decision, BindingDecision::RejectedInflight);
    assert!(!decision.accepted());

    let binding = store.get("sbx-1", unix(60)).await.unwrap().expect("held");
    assert_eq!(binding.execution_id, "exec-1");
    assert_eq!(
        binding.node.id, "node-a",
        "the refused launch must not have moved the sandbox"
    );
}

pub async fn a_newer_launch_supersedes_a_reservation_past_its_launch_window<S: BindingStore>(
    store: &S,
) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();

    let decision = store
        .record("sbx-1", reservation("node-b", "exec-2"), unix(200))
        .await
        .unwrap();
    assert_eq!(
        decision,
        BindingDecision::Superseded,
        "a reservation nobody finished has to be recoverable"
    );
    let binding = store.get("sbx-1", unix(200)).await.unwrap().expect("held");
    assert_eq!(binding.execution_id, "exec-2");
    assert_eq!(binding.node.id, "node-b");
}

pub async fn a_heartbeat_naming_a_newer_run_than_a_fresh_reservation_is_refused<S: BindingStore>(
    store: &S,
) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();

    let decisions = store
        .reconcile_node(node("node-b"), vec![roster("sbx-1", "exec-2")], unix(60))
        .await
        .unwrap();
    assert_eq!(
        decisions,
        vec![("sbx-1".to_string(), BindingDecision::RejectedInflight)]
    );
    let binding = store.get("sbx-1", unix(60)).await.unwrap().expect("held");
    assert_eq!(binding.execution_id, "exec-1");
    assert_eq!(binding.state, BindingState::Starting);
}

pub async fn a_heartbeat_naming_a_newer_run_than_a_stale_reservation_takes_it_over<
    S: BindingStore,
>(
    store: &S,
) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();

    let decisions = store
        .reconcile_node(node("node-b"), vec![roster("sbx-1", "exec-2")], unix(200))
        .await
        .unwrap();
    assert_eq!(
        decisions,
        vec![("sbx-1".to_string(), BindingDecision::Superseded)]
    );
    let binding = store.get("sbx-1", unix(200)).await.unwrap().expect("held");
    assert_eq!(binding.execution_id, "exec-2");
    assert_eq!(binding.state, BindingState::Confirmed);
}

pub async fn releasing_a_reservation_removes_it<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", reservation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .release_reservation("sbx-1", "exec-1", unix(0))
            .await
            .unwrap(),
        BindingDeleteOutcome::Deleted
    );
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_none());
}

pub async fn releasing_refuses_to_withdraw_a_confirmation<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", confirmation("node-a", "exec-1"), unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .release_reservation("sbx-1", "exec-1", unix(0))
            .await
            .unwrap(),
        BindingDeleteOutcome::RejectedConfirmed,
        "a launch that failed after its node acknowledged the sandbox unrouted a live runtime"
    );
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
}

pub async fn releasing_refuses_another_incarnations_reservation<S: BindingStore>(store: &S) {
    store
        .record("sbx-1", reservation("node-a", "exec-2"), unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .release_reservation("sbx-1", "exec-1", unix(0))
            .await
            .unwrap(),
        BindingDeleteOutcome::RejectedStale
    );
    assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
}

pub async fn releasing_an_absent_reservation_is_a_noop<S: BindingStore>(store: &S) {
    assert_eq!(
        store
            .release_reservation("never-reserved", "exec-1", unix(0))
            .await
            .unwrap(),
        BindingDeleteOutcome::Absent
    );
}

pub async fn a_launch_reservation_is_claimed_by_the_first_asker<S: BindingStore>(store: &S) {
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-1", unix(0))
            .await
            .unwrap(),
        LaunchReservationOutcome::Claimed
    );
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-1", unix(1))
            .await
            .unwrap(),
        LaunchReservationOutcome::Claimed,
        "the launch holding the id may say so again"
    );
}

pub async fn a_second_launch_is_told_which_one_holds_the_id<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    let outcome = store
        .reserve_launch("sbx-1", "exec-2", unix(1))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        LaunchReservationOutcome::HeldElsewhere {
            execution_id: "exec-1".to_string()
        },
        "the later launch has to be able to wait for the one that holds the id"
    );
    assert!(!outcome.claimed());
}

pub async fn a_reservation_past_its_window_is_taken_over<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    let outcome = store
        .reserve_launch("sbx-1", "exec-2", unix(600))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        LaunchReservationOutcome::ClaimedFromExpired {
            execution_id: "exec-1".to_string()
        },
        "a replica that died mid-launch must not hold a sandbox id forever"
    );
    assert!(outcome.claimed());
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-3", unix(601))
            .await
            .unwrap(),
        LaunchReservationOutcome::HeldElsewhere {
            execution_id: "exec-2".to_string()
        },
        "the launch that took the id over now holds it"
    );
}

pub async fn releasing_a_launch_reservation_frees_the_id<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .release_launch("sbx-1", "exec-1", unix(1))
            .await
            .unwrap(),
        BindingDeleteOutcome::Deleted
    );
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-2", unix(2))
            .await
            .unwrap(),
        LaunchReservationOutcome::Claimed,
        "a launch that failed and gave the id back must not keep excluding the next one"
    );
    assert_eq!(
        store
            .release_launch("sbx-2", "exec-1", unix(3))
            .await
            .unwrap(),
        BindingDeleteOutcome::Absent
    );
}

pub async fn releasing_another_launchs_reservation_is_refused<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .release_launch("sbx-1", "exec-2", unix(1))
            .await
            .unwrap(),
        BindingDeleteOutcome::RejectedStale
    );
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-3", unix(2))
            .await
            .unwrap(),
        LaunchReservationOutcome::HeldElsewhere {
            execution_id: "exec-1".to_string()
        },
        "the refused release must have left the reservation where it was"
    );
}

pub async fn reaping_removes_reservations_past_their_window_only<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-old", "exec-1", unix(0))
        .await
        .unwrap();
    store
        .reserve_launch("sbx-fresh", "exec-2", unix(550))
        .await
        .unwrap();

    assert_eq!(store.reap_expired_launches(unix(600)).await.unwrap(), 1);

    assert_eq!(
        store
            .reserve_launch("sbx-old", "exec-3", unix(601))
            .await
            .unwrap(),
        LaunchReservationOutcome::Claimed,
        "the reaped reservation must be gone, not merely unreadable"
    );
    assert_eq!(
        store
            .reserve_launch("sbx-fresh", "exec-4", unix(602))
            .await
            .unwrap(),
        LaunchReservationOutcome::HeldElsewhere {
            execution_id: "exec-2".to_string()
        },
        "a reservation inside its window is not residue"
    );
}

pub async fn reaping_leaves_a_reservation_taken_over_since_it_expired_alone<S: BindingStore>(
    store: &S,
) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-2", unix(601))
            .await
            .unwrap(),
        LaunchReservationOutcome::ClaimedFromExpired {
            execution_id: "exec-1".to_string()
        },
    );

    assert_eq!(
        store.reap_expired_launches(unix(602)).await.unwrap(),
        0,
        "the id is held by a launch that started after the expiry a sweep would reap"
    );
    assert_eq!(
        store
            .reserve_launch("sbx-1", "exec-3", unix(603))
            .await
            .unwrap(),
        LaunchReservationOutcome::HeldElsewhere {
            execution_id: "exec-2".to_string()
        },
        "a sweep that reaped this would hand the id to a third launch while the second runs"
    );
}

pub async fn a_launch_reservation_leaves_the_routing_record_alone<S: BindingStore>(store: &S) {
    store
        .reserve_launch("sbx-1", "exec-1", unix(0))
        .await
        .unwrap();
    assert!(
        store.get("sbx-1", unix(0)).await.unwrap().is_none(),
        "a reservation names no node, so it must not be readable as a routing record"
    );

    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::ZERO,
                state: BindingState::Confirmed,
            },
            unix(1),
        )
        .await
        .unwrap();
    store
        .release_launch("sbx-1", "exec-1", unix(2))
        .await
        .unwrap();
    let binding = store.get("sbx-1", unix(3)).await.unwrap().expect("bound");
    assert_eq!(
        binding.node.id, "node-a",
        "giving the launch reservation back must not retire the routing record it produced"
    );
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
            a_reservation_reads_back_as_starting,
            a_confirmation_over_a_reservation_promotes_it,
            a_heartbeat_that_has_not_heard_of_a_reservation_leaves_it_alone,
            an_empty_roster_still_leaves_a_reservation_alone,
            a_heartbeat_that_names_a_reservation_confirms_it,
            a_newer_launch_does_not_supersede_a_reservation_still_in_flight,
            a_newer_launch_supersedes_a_reservation_past_its_launch_window,
            a_heartbeat_naming_a_newer_run_than_a_fresh_reservation_is_refused,
            a_heartbeat_naming_a_newer_run_than_a_stale_reservation_takes_it_over,
            releasing_a_reservation_removes_it,
            releasing_refuses_to_withdraw_a_confirmation,
            releasing_refuses_another_incarnations_reservation,
            releasing_an_absent_reservation_is_a_noop,
            a_launch_reservation_is_claimed_by_the_first_asker,
            a_second_launch_is_told_which_one_holds_the_id,
            a_reservation_past_its_window_is_taken_over,
            releasing_a_launch_reservation_frees_the_id,
            releasing_another_launchs_reservation_is_refused,
            reaping_removes_reservations_past_their_window_only,
            reaping_leaves_a_reservation_taken_over_since_it_expired_alone,
            a_launch_reservation_leaves_the_routing_record_alone,
        );
    };
}

pub(crate) use {binding_store_contract, binding_store_contract_suite};
