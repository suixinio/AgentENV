//! The double write, one guard at a time.
//!
//! 🔴 Each doubled path makes its own central call, and each of those calls is
//! a separate guard. A suite that only ever broke "the catalog" as a whole
//! covered all of them with one test and none of them individually — every
//! guard could be deleted on its own and the suite stayed green. These tests
//! break one call at a time.

use std::sync::Arc;

use super::test_doubles::{
    commit_for, committed_record, record_for, CentralCall, ScriptedCatalog, ScriptedCentral,
};
use super::*;
use crate::snapshot::repository::backends::central::CatalogRefusal;
use crate::snapshot::types::TemplateBuildStatus;

struct Fixture {
    _dir: tempfile::TempDir,
    central: Arc<ScriptedCentral>,
    object_store: Arc<ScriptedCatalog>,
    backlog: Arc<MirrorBacklog>,
    dual: DualWriteCatalog,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let central = Arc::new(ScriptedCentral::default());
        let object_store = Arc::new(ScriptedCatalog::default());
        let backlog = MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open");
        // This fixture's object store has no history, and saying so is what
        // lets the read-side tests below be about debt rather than about a
        // backfill nobody ran. See `backlog::tests::backlog`.
        backlog
            .queue_history_toward_central(&ScriptedCatalog::default())
            .await
            .expect("an empty object store has no history to queue");
        let dual = DualWriteCatalog::new(
            Arc::clone(&central) as Arc<dyn CentralCatalogWrites>,
            Arc::clone(&object_store) as Arc<dyn SnapshotCatalog>,
            Arc::clone(&backlog),
        );
        Self {
            _dir: dir,
            central,
            object_store,
            backlog,
            dual,
        }
    }

    fn targets(&self) -> MirrorTargets {
        MirrorTargets::object_store(Arc::clone(&self.object_store) as Arc<dyn SnapshotCatalog>)
            .with_central(Arc::clone(&self.central) as Arc<dyn CentralCatalogWrites>)
    }

    fn owed_to_central(&self) -> u64 {
        self.backlog.lag_toward(MirrorDirection::Central)
    }

    fn owed_to_object_store(&self) -> u64 {
        self.backlog.lag_toward(MirrorDirection::ObjectStore)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// D-3: an unreachable central catalog is a debt, not a failure
// ─────────────────────────────────────────────────────────────────────────────

/// 🔴 The decision this batch turns on. A scheduler nobody can reach must not
/// fail a user's create — the write is recoverable, the object store is the
/// side that answers reads, and the alternative is the failure path that
/// deleted a paused sandbox's bytes.
#[tokio::test]
async fn an_unreachable_central_catalog_does_not_fail_a_create() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);
    let id = SnapshotId::generate();

    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("an unreachable central catalog must not fail the create");

    assert_eq!(fixture.owed_to_central(), 1, "the central write is owed");
    assert_eq!(fixture.owed_to_object_store(), 0);
    assert!(
        fixture
            .object_store
            .calls()
            .contains(&format!("create:{id}")),
        "the object store must still have been written: {:?}",
        fixture.object_store.calls()
    );
}

/// The debt is payable: the same operation replays into the catalog once it is
/// back, which is what makes recording it instead of failing defensible.
#[tokio::test]
async fn a_create_owed_to_the_central_catalog_replays_into_it() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);
    let id = SnapshotId::generate();
    fixture.dual.create(record_for(&id)).await.expect("create");
    assert!(fixture.central.holds(&id).is_none());

    fixture.central.reachable_again();
    let pass = fixture
        .backlog
        .drain_once(&fixture.targets())
        .await
        .expect("the pass should run");

    assert_eq!(pass.repaired, 1);
    assert_eq!(fixture.owed_to_central(), 0);
    assert!(
        fixture.central.holds(&id).is_some(),
        "the replay must have put the row into the central catalog"
    );
}

/// 🔴 `publish_commit` makes two central calls and the second one has a fence:
/// it only flips a row that is `building`. Running it against a catalog that
/// never took the opening statement would be refused for a reason that says
/// nothing about this snapshot, and counted as a divergence that is not one.
#[tokio::test]
async fn an_unreachable_opening_statement_does_not_run_the_commit() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("an unreachable central catalog must not fail the publish");

    assert!(
        !fixture
            .central
            .calls()
            .iter()
            .any(|call| call.starts_with("commit:")),
        "the commit must not be attempted against a row that was never opened: {:?}",
        fixture.central.calls()
    );
    assert_eq!(fixture.owed_to_central(), 1);
    assert_eq!(
        fixture.backlog.diverged_toward(MirrorDirection::Central),
        0,
        "an unreachable catalog is a debt, not a disagreement"
    );
}

/// The other half of the same path: the row opened and only the flip was lost.
/// One entry covers both statements, because replaying it re-runs both and the
/// opening one answering `ALREADY_EXISTS` is the ordinary case.
#[tokio::test]
async fn an_unreachable_commit_owes_the_whole_publish() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Commit);
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("the publish must succeed");

    assert!(
        fixture
            .central
            .calls()
            .iter()
            .any(|call| call.starts_with("commit:")),
        "the commit must have been attempted: {:?}",
        fixture.central.calls()
    );
    assert_eq!(fixture.owed_to_central(), 1);
    assert!(
        fixture
            .central
            .holds(&id)
            .expect("the opening statement landed")
            .committed
            .is_none(),
        "the row is open and not committed"
    );

    fixture.central.reachable_again();
    let pass = fixture
        .backlog
        .drain_once(&fixture.targets())
        .await
        .expect("the pass should run");
    assert_eq!(pass.repaired, 1);
    assert!(
        fixture
            .central
            .holds(&id)
            .expect("the row is there")
            .committed
            .is_some(),
        "the replay must have flipped it"
    );
}

#[tokio::test]
async fn an_unreachable_central_catalog_does_not_fail_a_delete() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.central.unreachable_on(CentralCall::Delete);

    fixture
        .dual
        .delete_record(&committed_record(&id))
        .await
        .expect("an unreachable central catalog must not fail the delete");

    assert_eq!(fixture.owed_to_central(), 1);
    assert!(
        fixture
            .object_store
            .calls()
            .contains(&format!("delete_record:{id}")),
        "the object store must still have been written: {:?}",
        fixture.object_store.calls()
    );
}

#[tokio::test]
async fn an_unreachable_central_catalog_does_not_fail_a_build_error() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.central.unreachable_on(CentralCall::Fail);

    fixture
        .dual
        .mark_build_error(&id, TemplateBuildErrorReason::new("no"))
        .await
        .expect("an unreachable central catalog must not fail the build error");

    assert_eq!(fixture.owed_to_central(), 1);
    assert!(
        fixture
            .object_store
            .calls()
            .contains(&format!("mark_build_error:{id}")),
        "the object store must still have been written: {:?}",
        fixture.object_store.calls()
    );
}

/// 🔴 Both stores unreachable at once. The operation still succeeds: the queue
/// is durable and holds the whole commit, so the snapshot whose bytes are
/// already written is not thrown away over two transports being down. Both
/// debts are recorded and both are payable.
#[tokio::test]
async fn a_publish_neither_catalog_took_still_succeeds_and_owes_both() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);
    fixture.object_store.break_it();
    let id = SnapshotId::generate();

    let record = fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("a snapshot whose bytes are written is not thrown away over two dead transports");
    assert_eq!(record.id, id);
    assert!(record.committed.is_some());
    assert_eq!(fixture.owed_to_central(), 1);
    assert_eq!(fixture.owed_to_object_store(), 1);

    fixture.central.reachable_again();
    fixture.object_store.fix_it();
    let pass = fixture
        .backlog
        .drain_once(&fixture.targets())
        .await
        .expect("the pass should run");
    assert_eq!(pass.repaired, 2);
    assert_eq!(fixture.backlog.lag(), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// I3: an object store that will not take a write does not fail the operation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_broken_object_store_does_not_fail_a_create() {
    let fixture = Fixture::new().await;
    fixture.object_store.break_it();
    let id = SnapshotId::generate();

    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("a broken mirror must not fail a real create");

    assert_eq!(fixture.owed_to_object_store(), 1);
    assert_eq!(fixture.owed_to_central(), 0);
    assert!(fixture.central.holds(&id).is_some());
}

#[tokio::test]
async fn a_broken_object_store_does_not_fail_a_publish() {
    let fixture = Fixture::new().await;
    fixture.object_store.break_it();
    let id = SnapshotId::generate();

    let record = fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("a broken mirror must not fail a real publish");

    assert_eq!(record.id, id);
    assert_eq!(fixture.owed_to_object_store(), 1);
    assert!(fixture
        .central
        .holds(&id)
        .expect("the central catalog took it")
        .committed
        .is_some());
}

#[tokio::test]
async fn a_broken_object_store_does_not_fail_a_delete() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.object_store.break_it();

    fixture
        .dual
        .delete_record(&committed_record(&id))
        .await
        .expect("a broken mirror must not fail a real delete");

    assert_eq!(fixture.owed_to_object_store(), 1);
}

#[tokio::test]
async fn a_broken_object_store_does_not_fail_a_build_error() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating should work");
    fixture.object_store.break_it();

    fixture
        .dual
        .mark_build_error(&id, TemplateBuildErrorReason::new("no"))
        .await
        .expect("a broken mirror must not fail a real build error");

    assert_eq!(fixture.owed_to_object_store(), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// A refusal is still a refusal
// ─────────────────────────────────────────────────────────────────────────────

/// 🔴 The change is about transport, not about a catalog correctly saying no. A
/// name somebody else holds must fail the write and must not reach the object
/// store, which would otherwise end up holding a row the catalog refused.
#[tokio::test]
async fn a_taken_alias_from_the_central_catalog_still_fails_the_create() {
    let fixture = Fixture::new().await;
    fixture.central.refuse(
        CentralCall::Begin,
        CatalogRefusal::AliasTaken {
            holder: SnapshotId::generate().to_string(),
        },
    );
    let id = SnapshotId::generate();

    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect_err("a refusal the caller can act on must fail the write");

    assert!(
        fixture.object_store.calls().is_empty(),
        "the object store must not hold a row the catalog refused: {:?}",
        fixture.object_store.calls()
    );
    assert_eq!(
        fixture.backlog.lag(),
        0,
        "a refused write is owed to nobody"
    );
}

/// 🔴 I2 on the opening statement, and the `?` that carries it. A refusal this
/// build cannot read means the catalog is answering something the client never
/// asked; the publish fails and the object store is not touched, because a row
/// object storage holds that the catalog refused is the one disagreement the
/// batch has no story for.
#[tokio::test]
async fn a_refusal_the_opening_statement_cannot_read_fails_the_publish() {
    let fixture = Fixture::new().await;
    fixture
        .central
        .refuse(CentralCall::Begin, CatalogRefusal::Unknown(4242));
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect_err("a refusal this build cannot read must fail the publish");

    assert!(
        fixture.object_store.calls().is_empty(),
        "the object store must not hold a row the catalog refused: {:?}",
        fixture.object_store.calls()
    );
    assert_eq!(
        fixture.backlog.lag(),
        0,
        "a refused write is owed to nobody"
    );
}

/// The same on the second statement, which is a separate guard: the row was
/// opened, the flip was refused for a reason nothing here can act on, and the
/// object store still must not be written.
#[tokio::test]
async fn a_refusal_the_commit_cannot_read_fails_the_publish() {
    let fixture = Fixture::new().await;
    fixture
        .central
        .refuse(CentralCall::Commit, CatalogRefusal::ExecutionSuperseded);
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect_err("a refusal this build cannot read must fail the publish");

    assert!(
        fixture
            .central
            .calls()
            .iter()
            .any(|call| call.starts_with("commit:")),
        "the commit must have been the thing that refused: {:?}",
        fixture.central.calls()
    );
    assert!(
        fixture.object_store.calls().is_empty(),
        "the object store must not hold a row the catalog refused: {:?}",
        fixture.object_store.calls()
    );
    assert_eq!(fixture.backlog.lag(), 0);
}

/// 🔴 An alias the object store will not bind is the caller's problem: reads
/// come from the object store in this phase, so reporting success would hand
/// back a snapshot the user cannot reach by the name they asked for.
///
/// 🔴 And the central catalog does not get to keep what the object store
/// refused. It took this commit and bound this name; leaving that in place
/// binds PostgreSQL to the *new* snapshot while object storage still binds the
/// old one — measured, on a real cluster, as one name resolving to two
/// different snapshots depending on which catalog answered. The row was opened
/// by this very call, so taking it back restores what was there a moment ago.
#[tokio::test]
async fn an_alias_the_object_store_refuses_is_taken_back_from_the_central_catalog() {
    let fixture = Fixture::new().await;
    let holder = SnapshotId::generate();
    fixture.object_store.refuse_alias_to(holder.clone());
    let id = SnapshotId::generate();

    let error = fixture
        .dual
        .publish_commit(commit_for(&id, Some("contested")))
        .await
        .expect_err("an alias the object store refuses must reach the caller");
    assert!(
        matches!(error, RepositoryError::AliasConflict { ref existing, .. } if *existing == holder),
        "expected an alias conflict, got {error:?}"
    );

    assert!(
        fixture.central.holds(&id).is_none(),
        "the central catalog must not keep a row bound to a name the object store refused"
    );
    assert_eq!(
        fixture.owed_to_object_store(),
        0,
        "a refused alias is not a write to replay"
    );
    assert_eq!(
        fixture.owed_to_central(),
        0,
        "and it is not owed the other way either"
    );
    assert_eq!(
        fixture
            .backlog
            .diverged_toward(MirrorDirection::ObjectStore),
        0,
        "the undo settled it, so there is nothing left to disagree about"
    );
}

/// 🔴 The undo can fail too, and then the disagreement is real and has to be
/// written down. Without this the catalogs are left apart with every gauge
/// reading zero — which is the failure the divergence record exists for.
#[tokio::test]
async fn an_alias_undo_that_fails_leaves_the_disagreement_recorded() {
    let fixture = Fixture::new().await;
    fixture.object_store.refuse_alias_to(SnapshotId::generate());
    fixture.central.unreachable_on(CentralCall::Delete);
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, Some("contested")))
        .await
        .expect_err("an alias the object store refuses must reach the caller");

    assert!(
        fixture.central.holds(&id).is_some(),
        "this test is only meaningful while the undo actually failed"
    );
    assert_eq!(
        fixture
            .backlog
            .diverged_toward(MirrorDirection::ObjectStore),
        1,
        "the two catalogs now disagree about this snapshot and something has to say so"
    );
}

/// 🔴 P7's user-visible regression. `create` had no carve-out at all: twenty
/// concurrent creates for one template name each answered **202** while three
/// records existed, so seventeen callers were told a template had been created
/// that no catalog holds under that name. Under `write = "object_store"` those
/// seventeen are told no.
#[tokio::test]
async fn an_alias_the_object_store_refuses_on_create_is_the_callers_problem() {
    let fixture = Fixture::new().await;
    let holder = SnapshotId::generate();
    fixture.object_store.refuse_alias_to(holder.clone());
    let id = SnapshotId::generate();

    let mut record = record_for(&id);
    record.alias = Some(crate::snapshot::types::SnapshotAlias::parse("contested").expect("alias"));

    let error = fixture
        .dual
        .create(record)
        .await
        .expect_err("a name the object store will not bind must reach the caller");
    assert!(
        matches!(error, RepositoryError::AliasConflict { ref existing, .. } if *existing == holder),
        "expected an alias conflict, got {error:?}"
    );
    assert!(
        fixture.central.holds(&id).is_none(),
        "the central catalog must not keep a row for a create the object store refused"
    );
    assert_eq!(fixture.backlog.lag(), 0, "a refused create owes nothing");
}

/// 🔴 And nothing is queued for it either. The central catalog being
/// unreachable at the moment the object store refuses the name would otherwise
/// leave a `create` on the queue that binds, in PostgreSQL, the very name the
/// caller was just told they could not have — the same hijack, arriving one
/// compensator interval later.
#[tokio::test]
async fn a_create_the_object_store_refuses_queues_nothing_for_the_central_catalog() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);
    fixture.object_store.refuse_alias_to(SnapshotId::generate());

    let mut record = record_for(&SnapshotId::generate());
    record.alias = Some(crate::snapshot::types::SnapshotAlias::parse("contested").expect("alias"));

    fixture
        .dual
        .create(record)
        .await
        .expect_err("a name the object store will not bind must reach the caller");

    assert_eq!(
        fixture.owed_to_central(),
        0,
        "a write the caller was told failed must not be replayed into either store"
    );
}

/// 🔴 The status code, which the switch to `write = "both"` regressed. The
/// object-store path answers a lost race for a name with
/// [`RepositoryError::AliasConflict`]; the central path flattened it into a
/// `Backend` error, which the API layer turns into a **500** carrying the text
/// `central snapshot catalog refused 'publish_commit.commit'`. A client cannot
/// act on a 500, and the name of an internal statement is not the API's
/// business.
#[tokio::test]
async fn a_lost_alias_race_in_the_central_catalog_is_reported_as_a_conflict() {
    let fixture = Fixture::new().await;
    let holder = SnapshotId::generate();
    fixture.central.refuse(
        CentralCall::Commit,
        CatalogRefusal::AliasTaken {
            holder: holder.to_string(),
        },
    );
    let id = SnapshotId::generate();

    let error = fixture
        .dual
        .publish_commit(commit_for(&id, Some("contested")))
        .await
        .expect_err("a name the central catalog will not bind must fail the publish");

    match &error {
        RepositoryError::AliasConflict {
            alias,
            existing,
            new_id,
        } => {
            assert_eq!(alias, "contested", "the name the caller asked for");
            assert_eq!(existing, &holder);
            assert_eq!(new_id, &id);
        }
        other => panic!("a lost alias race must be an alias conflict, got {other:?}"),
    }
    assert!(
        !error.to_string().contains("publish_commit"),
        "the API must not be told the name of an internal statement: {error}"
    );
}

/// 🔴 And the row that failed publish opened does not stay behind. Left there
/// it is `building` forever: no resolving query sees it, no reaper collects it
/// until the batch that wires build admission, and no API call can delete it —
/// four of them were left on the cluster and came out only through psql.
#[tokio::test]
async fn a_commit_the_central_catalog_refuses_takes_back_the_row_it_opened() {
    let fixture = Fixture::new().await;
    fixture.central.refuse(
        CentralCall::Commit,
        CatalogRefusal::AliasTaken {
            holder: SnapshotId::generate().to_string(),
        },
    );
    let id = SnapshotId::generate();

    fixture
        .dual
        .publish_commit(commit_for(&id, Some("contested")))
        .await
        .expect_err("a name the central catalog will not bind must fail the publish");

    assert!(
        fixture.central.holds(&id).is_none(),
        "the opening statement's row must not outlive the publish that opened it"
    );
    assert!(
        fixture.object_store.calls().is_empty(),
        "the object store must not hold a row the catalog refused: {:?}",
        fixture.object_store.calls()
    );
}

/// 🔴 A row this call did *not* open is not this call's to delete. A template's
/// row was created when the template was; a publish that fails against it must
/// leave it where it is rather than destroying somebody else's state to tidy up
/// after itself.
#[tokio::test]
async fn a_refused_commit_leaves_a_row_it_did_not_open_alone() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.central.seed(record_for(&id));
    fixture.central.refuse(
        CentralCall::Commit,
        CatalogRefusal::AliasTaken {
            holder: SnapshotId::generate().to_string(),
        },
    );

    fixture
        .dual
        .publish_commit(commit_for(&id, Some("contested")))
        .await
        .expect_err("a name the central catalog will not bind must fail the publish");

    assert!(
        fixture.central.holds(&id).is_some(),
        "a row that was already there must survive a publish that failed against it"
    );
}

/// 🔴 P7's blocker, on the forward path. A controller that *answers* and says
/// the request will never be accepted is not an outage, and recording it as
/// debt is what made `mirror_lag{direction="central"}` a number that could
/// never reach zero: twenty entries replayed every thirty seconds forever,
/// `repair_failed_total{verdict="retry"}` climbing by twenty a pass, the lag
/// frozen — and the batch after this one is gated on exactly that number.
#[tokio::test]
async fn a_permanent_central_rejection_is_a_divergence_and_not_a_debt() {
    let fixture = Fixture::new().await;
    fixture.central.reject_permanently_on(CentralCall::Begin);
    let id = SnapshotId::generate();

    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("the object store still answers reads, so the create still succeeds");

    assert_eq!(
        fixture.owed_to_central(),
        0,
        "a write no replay can land is not debt; leaving it as debt is what froze the lag"
    );
    assert_eq!(
        fixture.backlog.diverged_toward(MirrorDirection::Central),
        1,
        "it is a disagreement, and it has to land somewhere an operator can see"
    );
}

/// The control face for the one above: a catalog nobody can *reach* is still
/// debt, and must stay debt. Reading every failure as permanent would abandon
/// writes over a scheduler that was restarting.
#[tokio::test]
async fn an_unreachable_central_catalog_is_still_a_debt() {
    let fixture = Fixture::new().await;
    fixture.central.unreachable_on(CentralCall::Begin);

    fixture
        .dual
        .create(record_for(&SnapshotId::generate()))
        .await
        .expect("an unreachable catalog must not fail the create");

    assert_eq!(fixture.owed_to_central(), 1);
    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 0);
}

/// 🔴 The rollback that follows must not delete the bytes. The central catalog
/// holds a `ready` row for this snapshot; asking only the object store — which
/// has nothing — answers "not owned" and destroys a live snapshot's artifacts.
#[tokio::test]
async fn artifacts_survive_a_failure_the_central_catalog_committed_through() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("publishing should work");

    assert!(
        fixture
            .dual
            .retains_artifacts_on_publish_failure(&id)
            .await
            .expect("the question should be answerable"),
        "a snapshot the central catalog has committed still owns its bytes"
    );
}

#[tokio::test]
async fn artifacts_nobody_committed_are_not_retained() {
    let fixture = Fixture::new().await;
    assert!(!fixture
        .dual
        .retains_artifacts_on_publish_failure(&SnapshotId::generate())
        .await
        .expect("the question should be answerable"));
}

// ─────────────────────────────────────────────────────────────────────────────
// F-1: lag zero is not agreement
// ─────────────────────────────────────────────────────────────────────────────

/// 🔴 The batch's permanent gap, recorded rather than only counted.
/// `try_start_build` writes object storage alone, so the central row stays
/// `waiting` while this one moves to `building` — for good, with nothing owed.
#[tokio::test]
async fn starting_a_build_records_the_divergence_it_creates() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating should work");

    let record = fixture
        .dual
        .try_start_build(&id)
        .await
        .expect("the object store decides");
    assert!(matches!(
        record.source,
        crate::snapshot::types::SnapshotSource::Template { ref build }
            if build.status == TemplateBuildStatus::Building
    ));

    assert_eq!(
        fixture.backlog.lag(),
        0,
        "nothing the compensator could replay would close it"
    );
    assert_eq!(
        fixture.backlog.diverged_toward(MirrorDirection::Central),
        1,
        "and something other than the lag has to know that"
    );
}

/// A build start the object store itself refused is not a divergence: neither
/// catalog moved, so they still agree.
#[tokio::test]
async fn a_build_start_the_object_store_refused_is_not_a_divergence() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.object_store.fail(&id, true);

    fixture
        .dual
        .try_start_build(&id)
        .await
        .expect_err("the object store refused");

    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 0);
}

/// 🔴 A refusal the central row could not take is a disagreement in its own
/// right, and not only when a build start put the row there. A commit refused
/// because the row is not `building` leaves object storage `ready` and
/// PostgreSQL behind — with nothing owed, because no replay would change the
/// answer — and nothing else in the process would ever say so.
#[tokio::test]
async fn a_commit_the_central_catalog_refused_is_a_divergence_on_its_own() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture.central.refuse(
        CentralCall::Commit,
        CatalogRefusal::StatusMismatch {
            observed: "waiting".to_string(),
        },
    );

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("the object store answers reads, so the publish still succeeds");

    assert_eq!(
        fixture.backlog.lag(),
        0,
        "there is nothing here a replay could settle"
    );
    assert_eq!(
        fixture.backlog.diverged_toward(MirrorDirection::Central),
        1,
        "but the two catalogs disagree about this snapshot, and the switch reads this"
    );
}

/// The commit that follows a build start finds a row it cannot flip. Counted
/// against the same snapshot, so one disagreement stays one.
#[tokio::test]
async fn a_row_the_commit_could_not_advance_is_the_same_divergence() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating should work");
    fixture.dual.try_start_build(&id).await.expect("starting");
    fixture.central.refuse(
        CentralCall::Commit,
        CatalogRefusal::StatusMismatch {
            observed: "waiting".to_string(),
        },
    );

    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("the object store answers reads, so the publish still succeeds");

    assert_eq!(
        fixture.backlog.diverged_toward(MirrorDirection::Central),
        1,
        "one snapshot the catalogs disagree about, discovered twice"
    );
}

/// 🔴 The switch the 2c gate performs, refused while the catalogs disagree.
///
/// Without this, a cluster holding template snapshots reads a mirror lag of
/// zero — nothing is owed, because nothing can be replayed — and authorises
/// moving reads onto a PostgreSQL whose rows for every one of those templates
/// are still `waiting`, which is to say invisible to the API.
#[tokio::test]
async fn moving_reads_to_postgres_is_refused_while_the_central_catalog_disagrees() {
    let fixture = Fixture::new().await;
    fixture
        .backlog
        .guard_read_side(CatalogReadSide::ObjectStore)
        .await
        .expect("the first start records the side");

    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating should work");
    fixture.dual.try_start_build(&id).await.expect("starting");
    assert_eq!(fixture.backlog.lag(), 0, "the lag alone would allow it");

    let error = fixture
        .backlog
        .guard_read_side(CatalogReadSide::Postgres)
        .await
        .expect_err("the switch must be refused while the catalogs disagree");
    assert!(
        error
            .to_string()
            .contains("1 snapshot(s) recorded as diverged"),
        "the refusal must say what disagrees: {error}"
    );

    // The control: with the disagreement resolved, the same switch is allowed.
    fixture
        .dual
        .delete_record(&committed_record(&id))
        .await
        .expect("deleting should work");
    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 0);
    fixture
        .backlog
        .guard_read_side(CatalogReadSide::Postgres)
        .await
        .expect("a mirror that agrees must let the switch through");
}

/// 🔴 A divergence in one direction does not pin the switch in the other. The
/// central catalog being behind says nothing about whether object storage is,
/// and refusing a rollback over it would turn a mirror that is behind in a
/// direction nobody is reading into a node that will not start.
#[tokio::test]
async fn a_central_divergence_does_not_refuse_the_rollback_to_object_storage() {
    let fixture = Fixture::new().await;
    fixture
        .backlog
        .guard_read_side(CatalogReadSide::Postgres)
        .await
        .expect("recording the side should work");

    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating");
    fixture.dual.try_start_build(&id).await.expect("starting");
    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 1);

    fixture
        .backlog
        .guard_read_side(CatalogReadSide::ObjectStore)
        .await
        .expect("object storage owes nothing, so reading from it loses nothing");
}

/// Deleting a snapshot settles whatever the two catalogs disagreed about it:
/// both are losing the row.
#[tokio::test]
async fn deleting_a_snapshot_clears_what_the_catalogs_disagreed_about() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .create(record_for(&id))
        .await
        .expect("creating");
    fixture.dual.try_start_build(&id).await.expect("starting");
    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 1);

    fixture
        .dual
        .delete_record(&committed_record(&id))
        .await
        .expect("deleting should work");

    assert_eq!(fixture.backlog.diverged_toward(MirrorDirection::Central), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

/// Reads come from the object store in this phase, and only from it. A fallback
/// to the catalog would make the acceptance number — object-store requests per
/// listing — depend on which store happened to be up.
#[tokio::test]
async fn reads_come_from_the_object_store() {
    let fixture = Fixture::new().await;
    let id = SnapshotId::generate();
    fixture
        .dual
        .publish_commit(commit_for(&id, None))
        .await
        .expect("publishing should work");

    fixture.object_store.break_reads();
    fixture
        .dual
        .get(&id.to_string())
        .await
        .expect_err("a read must fail with object storage rather than fall back to the catalog");
}

// ─────────────────────────────────────────────────────────────────────────────
// The refusal policy
// ─────────────────────────────────────────────────────────────────────────────

/// 🔴 The one refusal that must fail the write. Somebody else holds the name;
/// letting it through would commit a snapshot the user cannot reach by the name
/// they asked for, which is the defect the central catalog's unique index
/// exists to remove.
#[test]
fn a_taken_alias_fails_the_write() {
    assert!(matches!(
        policy_for(&CatalogRefusal::AliasTaken {
            holder: "somebody".to_string()
        }),
        RefusalPolicy::Fatal
    ));
}

/// 🔴 A refusal this build cannot read must not be assumed harmless. It means
/// the catalog is answering something this client did not ask, and treating
/// that as "the mirror is a little behind" would let an unknown class of
/// failure through as a success.
#[test]
fn a_refusal_this_build_cannot_read_fails_the_write() {
    for refusal in [
        CatalogRefusal::Unknown(4242),
        CatalogRefusal::ExecutionSuperseded,
        CatalogRefusal::GenerationMismatch { observed: Some(7) },
    ] {
        assert!(
            matches!(policy_for(&refusal), RefusalPolicy::Fatal),
            "{refusal} must not be waved through"
        );
    }
}

/// `publish_commit` opens a row before flipping it, and on the template path
/// the row already exists. That is the ordinary answer.
#[test]
fn an_existing_row_is_what_opening_one_was_for() {
    assert!(matches!(
        policy_for(&CatalogRefusal::AlreadyExists),
        RefusalPolicy::Satisfied
    ));
}

/// 🔴 The batch's known gap. Build admission is not wired, so the central row
/// for a template is still `waiting` when the commit arrives and the commit's
/// fence refuses it. Recorded and carried on with — object storage is still the
/// side that answers reads, so failing here would fail a publish nothing is
/// actually wrong with.
#[test]
fn a_row_the_catalog_could_not_advance_is_a_divergence_and_not_a_failure() {
    for refusal in [
        CatalogRefusal::StatusMismatch {
            observed: "waiting".to_string(),
        },
        CatalogRefusal::NotFound,
        CatalogRefusal::BuildInProgress {
            active_build_id: "b".to_string(),
        },
        CatalogRefusal::BuildQueueFull,
    ] {
        assert!(
            matches!(policy_for(&refusal), RefusalPolicy::Diverged),
            "{refusal} should be counted, not fatal"
        );
    }
}

/// Every refusal has a metric label, and no two share one — a reason that
/// silently merged into another would make the divergence counter unable to say
/// what happened.
#[test]
fn every_refusal_reason_has_its_own_label() {
    let labels = [
        CatalogRefusal::NotFound,
        CatalogRefusal::StatusMismatch {
            observed: String::new(),
        },
        CatalogRefusal::AliasTaken {
            holder: String::new(),
        },
        CatalogRefusal::GenerationMismatch { observed: None },
        CatalogRefusal::ExecutionSuperseded,
        CatalogRefusal::BuildInProgress {
            active_build_id: String::new(),
        },
        CatalogRefusal::BuildQueueFull,
        CatalogRefusal::AlreadyExists,
        CatalogRefusal::Unknown(0),
    ]
    .iter()
    .map(CatalogRefusal::as_metric_label)
    .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(labels.len(), 9);
}
