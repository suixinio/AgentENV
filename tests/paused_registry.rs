//! Behavioural tests for the PostgreSQL paused-sandbox registry.
//!
//! Everything interesting about this registry lives in its SQL — which row a
//! predicate matches, and which it deliberately does not — so none of it can be
//! checked without a real database. Point `AENV_PAUSED_REGISTRY_TEST_DSN` at a
//! throwaway PostgreSQL to run these; without it they skip.
//!
//! ```text
//! docker run -d --rm --name aenv-pg -e POSTGRES_PASSWORD=verify \
//!     -e POSTGRES_DB=aenv_registry -p 55432:5432 postgres:16-alpine
//! AENV_PAUSED_REGISTRY_TEST_DSN=postgres://postgres:verify@127.0.0.1:55432/aenv_registry \
//!     cargo test --test paused_registry
//! ```

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use uuid::Uuid;

use agentenv::orchestrator::{
    HeldSandbox, PausedRegistryError, PausedRegistryState, PausedSandboxEntry,
    PausedSandboxRegistry, PostgresPausedSandboxRegistry, ResumeClaim, SandboxMetadata,
};
use agentenv::snapshot::SnapshotId;
use agentenv::types::SandboxId;

/// Short enough that a test can outlive a lease without dragging.
const TEST_LEASE_SECS: f64 = 1.0;
/// Comfortably past `TEST_LEASE_SECS` on a loaded machine.
const PAST_LEASE: Duration = Duration::from_millis(1_600);

const NODE_A: &str = "node-a";
const NODE_B: &str = "node-b";

fn dsn() -> Option<String> {
    std::env::var("AENV_PAUSED_REGISTRY_TEST_DSN")
        .ok()
        .map(|dsn| dsn.trim().to_string())
        .filter(|dsn| !dsn.is_empty())
}

/// Skips the calling test when no database is configured.
///
/// A skipped test reports as passing, so a runner that is *supposed* to have a
/// database — CI, or a verification run — can set
/// `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1` and have a missing DSN fail loudly
/// instead of quietly turning this whole file into ten green no-ops.
macro_rules! require_db {
    () => {
        match dsn() {
            Some(dsn) => dsn,
            None => {
                assert!(
                    std::env::var("AENV_PAUSED_REGISTRY_TEST_REQUIRED").is_err(),
                    "AENV_PAUSED_REGISTRY_TEST_REQUIRED is set but \
                     AENV_PAUSED_REGISTRY_TEST_DSN is not: these tests would have been skipped"
                );
                eprintln!("skipping: set AENV_PAUSED_REGISTRY_TEST_DSN to run registry tests");
                return;
            }
        }
    };
}

async fn registry(dsn: &str, cluster_id: Uuid) -> PostgresPausedSandboxRegistry {
    PostgresPausedSandboxRegistry::connect(dsn, cluster_id, 4, TEST_LEASE_SECS)
        .await
        .expect("connect to the test registry")
}

/// A renewal for a sandbox with no deadline — the shape most of these tests
/// want, since they exercise the lease rather than reclamation.
fn held(sandbox_id: SandboxId) -> HeldSandbox {
    HeldSandbox {
        sandbox_id,
        expires_at: None,
    }
}

/// A renewal reporting a deadline `offset` from now. Negative means the sandbox
/// has already outlived it.
fn held_due(sandbox_id: SandboxId, offset: TimeDelta) -> HeldSandbox {
    HeldSandbox {
        sandbox_id,
        expires_at: Some(Utc::now() + offset),
    }
}

fn entry(sandbox_id: SandboxId, origin: &str) -> PausedSandboxEntry {
    PausedSandboxEntry {
        sandbox_id,
        cluster_id: Uuid::nil(),
        state: PausedRegistryState::Publishing,
        generation: 0,
        origin_node_id: origin.to_string(),
        claimed_by_node_id: None,
        snapshot_id: None,
        metadata: Some(SandboxMetadata::default()),
        paused_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Drives a sandbox to a durable `paused` row and returns the snapshot it names.
async fn pause_and_publish(
    registry: &PostgresPausedSandboxRegistry,
    sandbox_id: SandboxId,
    origin: &str,
) -> SnapshotId {
    let began = registry
        .begin_pause(&entry(sandbox_id, origin))
        .await
        .expect("begin pause");
    let snapshot = SnapshotId::generate();
    registry
        .complete_pause(&sandbox_id, began.generation, &snapshot)
        .await
        .expect("complete pause");

    snapshot
}

/// 🔴 The failure this registry exists to survive, and the one it used to make
/// worse: an upload that fails must not cost the sandbox the snapshot it
/// already had. Clearing the reference on `begin_pause` left the row naming
/// nothing while a perfectly good snapshot sat in the repository unreferenced,
/// so losing the origin node lost the sandbox outright.
#[tokio::test]
async fn a_failed_publish_keeps_the_snapshot_the_sandbox_already_had() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let first = pause_and_publish(&registry, sandbox_id, NODE_A).await;

    // Resume, then pause again — and this time the upload never lands.
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    let began = registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("begin second pause");
    assert_eq!(
        began.previous_snapshot_id.as_ref(),
        Some(&first),
        "the pause must report the snapshot it is superseding, so the caller can retire it later"
    );
    registry
        .mark_local_only(&sandbox_id, began.generation)
        .await
        .expect("downgrade to local-only");

    let row = registry
        .get(&sandbox_id)
        .await
        .expect("read row")
        .expect("row still exists");
    assert_eq!(row.state, PausedRegistryState::LocalOnly);
    assert_eq!(
        row.snapshot_id.as_ref(),
        Some(&first),
        "a failed publish must leave the previous snapshot referenced, not orphan it"
    );

    // And once the origin node stops renewing, that snapshot is what the
    // sandbox comes back from — degraded to the previous pause, but alive.
    tokio::time::sleep(PAST_LEASE).await;
    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    let ResumeClaim::Claimed { entry: claimed, .. } = claim else {
        panic!("a lapsed local-only row must be recoverable from its last snapshot");
    };
    assert_eq!(claimed.snapshot_id.as_ref(), Some(&first));
}

/// 🔴 The second-copy bug. A `running` row means some node is running the
/// sandbox; taking it over on any signal weaker than an expired lease starts a
/// second live copy. The gateway routes a resume to an arbitrary node whenever
/// the scheduler holds no binding, and bindings are in-memory with a 30s TTL and
/// are lost entirely when the scheduler restarts — so "nobody is bound to it"
/// arrives routinely for perfectly healthy sandboxes.
#[tokio::test]
async fn a_live_holder_cannot_have_its_sandbox_taken_away() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    match registry.claim_for_resume(&sandbox_id, NODE_B).await {
        Ok(ResumeClaim::Conflict { origin_node_id }) => assert_eq!(origin_node_id, NODE_A),
        other => panic!("a live holder must not be displaced, got {other:?}"),
    }
}

/// 🔴 The duplication this design exists to make impossible.
///
/// A lapsed lease proves the holder cannot reach this database. It does not
/// prove the holder is dead — a partitioned node keeps running every sandbox it
/// has, keeps being routed traffic, and keeps writing to its rootfs layers,
/// while its rows expire on schedule. Rebuilding one of those sandboxes on
/// another node produces two live copies diverging from the same snapshot, and
/// nothing downstream can merge them again.
///
/// So a live row stays with its holder however long the lease has been lapsed.
/// e2b refuses the same move from the other side of the same fact: a resume
/// that finds the sandbox in its store is a 409, not a placement.
#[tokio::test]
async fn a_live_sandbox_is_never_taken_over_on_a_lapsed_lease() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    tokio::time::sleep(PAST_LEASE).await;

    match registry.claim_for_resume(&sandbox_id, NODE_B).await {
        Ok(ResumeClaim::Conflict { origin_node_id }) => assert_eq!(origin_node_id, NODE_A),
        other => panic!("a live sandbox must never be rebuilt elsewhere on a timer, got {other:?}"),
    }
}

/// The parked half, which is what the lease is still for. `publishing` and
/// `local_only` name a node that already stopped the VM, so taking the sandbox
/// over cannot duplicate it — it only rewinds to the snapshot the previous
/// pause left behind. That loss is worth accepting to avoid stranding the
/// sandbox on a node that may never come back, so here the lease does decide.
#[tokio::test]
async fn a_parked_sandbox_moves_on_once_its_holder_stops_renewing() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    // A first pause that published, then a second that did not: the row is
    // `publishing` while still naming the older, durable snapshot.
    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("second pause");

    match registry.claim_for_resume(&sandbox_id, NODE_B).await {
        Ok(ResumeClaim::NotReady { origin_node_id }) => assert_eq!(origin_node_id, NODE_A),
        other => panic!("a live lease must keep the upload on its own node, got {other:?}"),
    }

    tokio::time::sleep(PAST_LEASE).await;

    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    let ResumeClaim::Claimed { previous_state, .. } = claim else {
        panic!("a parked sandbox whose holder went quiet must be recoverable elsewhere, got {claim:?}");
    };
    assert_eq!(
        previous_state,
        PausedRegistryState::Publishing,
        "the claim has to name the state it overrode: this one cost its holder an \
         unpublished pause, which is the whole reason the event is worth logging"
    );
}

/// How a live row is *actually* released: by the next process on the machine
/// that was holding it.
///
/// Being the successor is the proof no timeout can supply — the previous
/// process's VMs were its children, so a process that has just started on that
/// machine and holds nothing is looking at rows whose sandboxes are certainly
/// gone. The sandbox goes back to `paused` and its snapshot stays, so the next
/// resume rebuilds it anywhere.
#[tokio::test]
async fn a_successor_process_releases_what_the_previous_one_was_running() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let snapshot = pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    let released = registry
        .release_node_holdings(NODE_A)
        .await
        .expect("release holdings");
    assert_eq!(released.released, 1);
    assert_eq!(released.discarded, 0);

    let entry = registry
        .get(&sandbox_id)
        .await
        .expect("read back")
        .expect("row survives the release");
    assert_eq!(entry.state, PausedRegistryState::Paused);
    assert_eq!(
        entry.snapshot_id.as_ref(),
        Some(&snapshot),
        "the snapshot is the whole reason the sandbox survives the node"
    );

    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    assert!(
        matches!(claim, ResumeClaim::Claimed { .. }),
        "a released sandbox must be recoverable on any node"
    );
}

/// A live sandbox whose snapshot never published has its only artifacts on the
/// disk of the process that just died, and the resume that started it consumed
/// the paused record they belonged to. Keeping the row would leave something no
/// node can claim (a claim requires a snapshot) and no node will ever clear.
#[tokio::test]
async fn a_successor_process_discards_live_rows_that_never_published() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let began = registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("begin pause");
    registry
        .mark_local_only(&sandbox_id, began.generation)
        .await
        .expect("publish never landed");
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("resumed locally");

    let released = registry
        .release_node_holdings(NODE_A)
        .await
        .expect("release holdings");
    assert_eq!(released.released, 0);
    assert_eq!(released.discarded, 1);
    assert!(
        registry
            .get(&sandbox_id)
            .await
            .expect("read back")
            .is_none(),
        "a sandbox with nothing to rebuild from must not leave a row behind"
    );
}

/// The release is scoped to one node's own holdings twice over: another node's
/// live sandboxes are untouchable, and this node's parked rows are left exactly
/// as they were. Widening either would turn a routine restart into a cluster
/// event.
#[tokio::test]
async fn releasing_holdings_touches_nothing_but_this_nodes_live_rows() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;

    let live_elsewhere = SandboxId::new();
    pause_and_publish(&registry, live_elsewhere, NODE_B).await;
    registry
        .mark_running(&live_elsewhere, NODE_B)
        .await
        .expect("mark running on the other node");

    let parked_here = SandboxId::new();
    pause_and_publish(&registry, parked_here, NODE_A).await;

    let released = registry
        .release_node_holdings(NODE_A)
        .await
        .expect("release holdings");
    assert!(
        released.is_empty(),
        "a node with nothing live of its own must release nothing, got {released:?}"
    );

    assert_eq!(
        registry
            .get(&live_elsewhere)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Running,
        "another node's live sandbox must be untouched"
    );
    assert_eq!(
        registry
            .get(&parked_here)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Paused,
    );
}

/// A resume that was in flight when the process died is released by the node
/// that claimed it, not by the one whose disk holds the artifacts — the claim
/// deliberately leaves `origin_node_id` alone, so judging by it would let the
/// origin release a rebuild another node is midway through.
#[tokio::test]
async fn an_interrupted_resume_is_released_by_the_node_that_claimed_it() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    assert!(matches!(claim, ResumeClaim::Claimed { .. }));

    let by_origin = registry
        .release_node_holdings(NODE_A)
        .await
        .expect("release as the origin");
    assert!(
        by_origin.is_empty(),
        "the origin must not release a resume another node is running, got {by_origin:?}"
    );

    let by_claimer = registry
        .release_node_holdings(NODE_B)
        .await
        .expect("release as the claimer");
    assert_eq!(by_claimer.released, 1);
    assert_eq!(
        registry
            .get(&sandbox_id)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Paused,
    );
}

/// Renewal is what keeps a healthy node's sandboxes its own, and it must be
/// the holder doing it — otherwise any node could keep another node's dead
/// sandboxes out of reach indefinitely.
#[tokio::test]
async fn only_the_holder_can_renew_its_lease() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    assert_eq!(
        registry
            .renew_lease(NODE_B, &[held(sandbox_id)])
            .await
            .expect("renew as a stranger"),
        0,
        "a node must not be able to renew a lease on a sandbox it does not hold"
    );
    assert_eq!(
        registry
            .renew_lease(NODE_A, &[held(sandbox_id)])
            .await
            .expect("renew as the holder"),
        1
    );

    // Renewed, so the takeover that would otherwise succeed by now does not.
    tokio::time::sleep(Duration::from_millis(600)).await;
    registry
        .renew_lease(NODE_A, &[held(sandbox_id)])
        .await
        .expect("renew again");
    tokio::time::sleep(Duration::from_millis(600)).await;

    match registry.claim_for_resume(&sandbox_id, NODE_B).await {
        Ok(ResumeClaim::Conflict { .. }) => {}
        other => panic!("a renewed lease must keep the sandbox, got {other:?}"),
    }
}

/// A `paused` row has no holder by definition — nobody is running the sandbox
/// and its snapshot is durable — so it stays claimable straight away. Making
/// the lease apply here too would add a delay to every ordinary cross-node
/// resume for no gain.
#[tokio::test]
async fn a_paused_sandbox_is_claimable_immediately() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;

    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    assert!(matches!(claim, ResumeClaim::Claimed { .. }));
}

/// 🔴 An ordinary resume must not report itself as a takeover.
///
/// The claim is one conditional `UPDATE` that sets `state = 'resuming'`, and
/// `RETURNING` describes the row it produced — so the returned `state` reads
/// `Resuming` for every claim, whatever the row said a moment earlier. Deciding
/// the outcome from it labelled every routine resume "holder stopped renewing
/// its lease", which buried the rare claim that really does cost someone an
/// unpublished pause under the common one that costs nothing.
///
/// Nothing caught it, because every assertion in this file looked only at the
/// variant and the snapshot — never at what the claim said it replaced. This is
/// that assertion.
#[tokio::test]
async fn an_ordinary_claim_reports_no_takeover() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;

    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    let ResumeClaim::Claimed { previous_state, .. } = claim else {
        panic!("a durably paused sandbox is claimable, got {claim:?}");
    };
    assert_eq!(
        previous_state,
        PausedRegistryState::Paused,
        "no lease was in question here: the row was published and idle"
    );
}

/// 🔴 One node's resume must not erase another's in-flight one. A blind write
/// here cleared `claimed_by_node_id` mid-claim, after which both nodes believed
/// they held the sandbox and both brought it up.
#[tokio::test]
async fn marking_a_sandbox_running_cannot_erase_another_nodes_claim() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    let ResumeClaim::Claimed { .. } = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim")
    else {
        panic!("node B should have taken the claim");
    };

    // Node A resumes from its own disk at the same moment.
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    let row = registry
        .get(&sandbox_id)
        .await
        .expect("read row")
        .expect("row exists");
    assert_eq!(
        row.state,
        PausedRegistryState::Resuming,
        "node A must not have taken the row from node B's claim"
    );
    assert_eq!(row.claimed_by_node_id.as_deref(), Some(NODE_B));

    // The node that does hold the claim still completes normally.
    registry
        .mark_running(&sandbox_id, NODE_B)
        .await
        .expect("mark running as the claimer");
    let row = registry
        .get(&sandbox_id)
        .await
        .expect("read row")
        .expect("row exists");
    assert_eq!(row.state, PausedRegistryState::Running);
    assert_eq!(row.origin_node_id, NODE_B);
}

/// 🔴 Two clusters pointed at one registry database must not see each other's
/// sandboxes. Without the scope a node claims a foreign row, fails to find the
/// snapshot in its own repository, and deletes the row as dangling — taking the
/// other cluster's sandbox with it.
#[tokio::test]
async fn one_cluster_cannot_reach_anothers_sandboxes() {
    let dsn = require_db!();
    let ours = registry(&dsn, Uuid::new_v4()).await;
    let theirs = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&ours, sandbox_id, NODE_A).await;

    assert!(
        theirs.get(&sandbox_id).await.expect("read").is_none(),
        "another cluster's row must not be readable"
    );
    assert!(
        matches!(
            theirs.claim_for_resume(&sandbox_id, NODE_B).await,
            Ok(ResumeClaim::NotFound)
        ),
        "another cluster's sandbox must not be claimable"
    );

    theirs.remove(&sandbox_id).await.expect("remove");
    assert!(
        ours.get(&sandbox_id).await.expect("read").is_some(),
        "another cluster's delete must not remove our row"
    );

    // And it cannot take the row over by pausing the same ID either.
    let hijack = theirs.begin_pause(&entry(sandbox_id, NODE_B)).await;
    assert!(
        matches!(hijack, Err(PausedRegistryError::InvalidRecord { .. })),
        "a foreign cluster must not be able to rewrite the row, got {hijack:?}"
    );
    let row = ours
        .get(&sandbox_id)
        .await
        .expect("read")
        .expect("row survives");
    assert_eq!(row.origin_node_id, NODE_A);
}

/// A downgrade that quietly matches nothing leaves the row stuck in
/// `publishing`, and every resume from another node then reports an upload that
/// gave up long ago as still in progress.
#[tokio::test]
async fn a_downgrade_that_matches_nothing_is_reported() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let began = registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("begin pause");

    let stale = registry
        .mark_local_only(&sandbox_id, began.generation - 1)
        .await;
    assert!(
        matches!(stale, Err(PausedRegistryError::GenerationConflict { .. })),
        "a downgrade against a superseded generation must be reported, got {stale:?}"
    );

    registry
        .mark_local_only(&sandbox_id, began.generation)
        .await
        .expect("downgrade with the right generation");
}

/// A pause whose very first publish fails has nothing to fall back on, so the
/// row must stay unclaimable however long its lease has been lapsed — there is
/// no snapshot to rebuild from, and answering otherwise would hand a caller a
/// claim it cannot use.
#[tokio::test]
async fn a_sandbox_that_never_published_is_never_claimable() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let began = registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("begin pause");
    registry
        .mark_local_only(&sandbox_id, began.generation)
        .await
        .expect("downgrade");

    tokio::time::sleep(PAST_LEASE).await;

    match registry.claim_for_resume(&sandbox_id, NODE_B).await {
        Ok(ResumeClaim::NotReady { origin_node_id }) => assert_eq!(origin_node_id, NODE_A),
        other => panic!("a sandbox with no snapshot must stay on its own node, got {other:?}"),
    }
}

/// A resume that fails after claiming has to put the sandbox back, or one bad
/// attempt parks it until the lease lapses.
#[tokio::test]
async fn releasing_a_claim_puts_the_sandbox_back() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    let ResumeClaim::Claimed { entry: claimed, .. } = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim")
    else {
        panic!("claim should have been granted");
    };

    registry
        .release_claim(&sandbox_id, claimed.generation)
        .await
        .expect("release");

    let row = registry
        .get(&sandbox_id)
        .await
        .expect("read")
        .expect("row exists");
    assert_eq!(row.state, PausedRegistryState::Paused);
    assert_eq!(row.claimed_by_node_id, None);
}

/// 🔴 The signal the running-sandbox reaper is built on. A node may only judge
/// its live copy against a registry row once the registry has confirmed the row
/// is about that copy; without a confirmation an absent row means nothing, and
/// reading it as "the cluster moved on" tears down sandboxes that were simply
/// created here and never announced.
#[tokio::test]
async fn marking_an_untracked_sandbox_running_reports_that_it_is_untracked() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;

    let confirmed = registry
        .mark_running(&SandboxId::new(), NODE_A)
        .await
        .expect("mark running");

    assert!(
        !confirmed,
        "a sandbox with no row must not be reported as tracked"
    );
}

#[tokio::test]
async fn marking_a_tracked_sandbox_running_reports_the_node_as_holder() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();
    pause_and_publish(&registry, sandbox_id, NODE_A).await;

    let confirmed = registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    assert!(confirmed, "the row now names this node as the holder");
}

/// A refusal has to be distinguishable from a success, or the node would enrol
/// a sandbox it does not hold and then reconcile the wrong copy away.
#[tokio::test]
async fn marking_running_reports_a_refusal_when_another_node_holds_the_claim() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();
    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");

    let confirmed = registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    assert!(
        !confirmed,
        "another node holds the claim, so this node is not the holder"
    );
}

/// Reconciliation asks about a node's whole roster at once. The batch has to
/// answer exactly what a row-at-a-time read would: present means present,
/// absent means "no row" — never "not looked at".
#[tokio::test]
async fn a_batch_read_reports_only_the_sandboxes_that_have_rows() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let tracked = SandboxId::new();
    let also_tracked = SandboxId::new();
    let untracked = SandboxId::new();
    pause_and_publish(&registry, tracked, NODE_A).await;
    pause_and_publish(&registry, also_tracked, NODE_B).await;

    let rows = registry
        .get_many(&[tracked, untracked, also_tracked])
        .await
        .expect("batch read");

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.get(&tracked)
            .map(|entry| entry.origin_node_id.as_str()),
        Some(NODE_A)
    );
    assert_eq!(
        rows.get(&also_tracked)
            .map(|entry| entry.origin_node_id.as_str()),
        Some(NODE_B)
    );
    assert!(
        !rows.contains_key(&untracked),
        "a sandbox with no row must be absent, which is how the caller reads 'the cluster does not track it'"
    );
}

/// 🔴 The batch is what reconciliation tears sandboxes down on, so it must be
/// scoped to this cluster just as tightly as every other read. Without the
/// filter, one cluster's reconciliation reads another cluster's rows and
/// concludes its own sandboxes have moved on.
#[tokio::test]
async fn a_batch_read_cannot_see_another_clusters_sandboxes() {
    let dsn = require_db!();
    let ours = registry(&dsn, Uuid::new_v4()).await;
    let theirs = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();
    pause_and_publish(&theirs, sandbox_id, NODE_A).await;

    let rows = ours.get_many(&[sandbox_id]).await.expect("batch read");

    assert!(rows.is_empty(), "another cluster's row must be invisible");
    assert!(
        theirs.get(&sandbox_id).await.expect("read").is_some(),
        "and untouched"
    );
}

#[tokio::test]
async fn a_batch_read_of_nothing_asks_nothing() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;

    let rows = registry.get_many(&[]).await.expect("batch read");

    assert!(rows.is_empty());
}

/// 🔴 The one thing that keeps a decommissioned machine's sandboxes from being
/// stranded forever.
///
/// Nothing else can release them: `release_node_holdings` needs a successor
/// process on that machine, and `claim_for_resume` refuses live rows outright.
/// So the cluster steps in when the sandbox has outlived the deadline its own
/// user gave it *and* nobody has renewed for it since — which is enforcing the
/// timeout, not guessing whether the node is dead.
///
/// e2b puts the same decision in its control-plane evictor, which runs off a
/// cluster-wide expiry index and drops the sandbox from its store even when the
/// node cannot be reached to be told.
#[tokio::test]
async fn a_sandbox_that_outlived_its_deadline_on_a_silent_node_is_reclaimed() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let snapshot = pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::seconds(-1))])
        .await
        .expect("report a deadline that has already passed");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert_eq!(reclaimed.released, 1);
    assert_eq!(reclaimed.discarded, 0);

    let entry = registry
        .get(&sandbox_id)
        .await
        .expect("read back")
        .expect("row survives");
    assert_eq!(entry.state, PausedRegistryState::Paused);
    assert_eq!(entry.snapshot_id.as_ref(), Some(&snapshot));
}

/// The lease is only half the condition. A node can go silent while its
/// sandboxes still have hours to run — that is a partition, and taking those
/// sandboxes would duplicate them.
#[tokio::test]
async fn a_sandbox_still_within_its_deadline_survives_a_silent_node() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::hours(1))])
        .await
        .expect("report a deadline an hour out");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert!(
        reclaimed.is_empty(),
        "a sandbox with time left must not be taken from a node that is merely quiet, got {reclaimed:?}"
    );
    assert_eq!(
        registry
            .get(&sandbox_id)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Running,
    );
}

/// The other half. A node that is still renewing evicts its own expired
/// sandboxes — pausing them properly and publishing a fresh snapshot, which is
/// strictly the better outcome. Stepping in front of that would rewind the
/// sandbox to an older snapshot for no reason.
#[tokio::test]
async fn an_expired_sandbox_stays_with_a_node_that_is_still_reporting() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::seconds(-1))])
        .await
        .expect("renew with a deadline that has passed");

    // No sleep: the lease this renewal just issued is still live.
    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert!(
        reclaimed.is_empty(),
        "a reporting node must get to evict its own sandbox, got {reclaimed:?}"
    );
}

/// A sandbox asked never to expire has no deadline to outlive, so no amount of
/// silence makes it reclaimable. Same for a row whose holder has not renewed
/// since the deadline column existed — an unknown deadline reads as no deadline,
/// which is the safe direction.
#[tokio::test]
async fn a_sandbox_with_no_deadline_is_never_reclaimed() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    registry
        .renew_lease(NODE_A, &[held(sandbox_id)])
        .await
        .expect("renew without a deadline");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert!(
        reclaimed.is_empty(),
        "a sandbox with no deadline must never be reclaimed, got {reclaimed:?}"
    );
}

/// Reclamation is scoped to live rows. Parked ones already have a mechanism —
/// the lease lets another node take them over — and rewriting them here would
/// bypass the `NotReady` redirect that keeps a still-publishing snapshot on its
/// own node.
#[tokio::test]
async fn reclamation_leaves_parked_rows_alone() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;

    let paused = SandboxId::new();
    pause_and_publish(&registry, paused, NODE_A).await;
    registry
        .renew_lease(NODE_A, &[held_due(paused, TimeDelta::seconds(-1))])
        .await
        .expect("renew");

    let publishing = SandboxId::new();
    registry
        .begin_pause(&entry(publishing, NODE_A))
        .await
        .expect("begin pause");
    registry
        .renew_lease(NODE_A, &[held_due(publishing, TimeDelta::seconds(-1))])
        .await
        .expect("renew");

    // The shape that actually tempts the predicate: parked, but *with* a
    // snapshot behind it. A pause that published once and then failed to
    // publish again leaves exactly this. Reclaiming it would mark it `paused`,
    // another node would claim it and rebuild from the older snapshot, and the
    // newer artifacts still sitting on the origin node would be thrown away —
    // all while the `NotReady` redirect that exists to prevent precisely that
    // is bypassed.
    let local_only = SandboxId::new();
    pause_and_publish(&registry, local_only, NODE_A).await;
    let second = registry
        .begin_pause(&entry(local_only, NODE_A))
        .await
        .expect("second pause");
    registry
        .mark_local_only(&local_only, second.generation)
        .await
        .expect("second publish never landed");
    registry
        .renew_lease(NODE_A, &[held_due(local_only, TimeDelta::seconds(-1))])
        .await
        .expect("renew");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert!(
        reclaimed.is_empty(),
        "parked rows are the lease's business, not reclamation's, got {reclaimed:?}"
    );
    assert_eq!(
        registry
            .get(&publishing)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Publishing,
    );
    assert_eq!(
        registry
            .get(&local_only)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::LocalOnly,
        "a parked row with a snapshot must not be handed to the cluster behind its origin's back"
    );
    assert_eq!(
        registry
            .get(&paused)
            .await
            .expect("read back")
            .expect("row")
            .state,
        PausedRegistryState::Paused,
    );
}

/// An expired live row with nothing published behind it leaves nothing to
/// rebuild, so it is deleted rather than parked — the same call reclamation's
/// sibling makes, for the same reason.
#[tokio::test]
async fn reclamation_discards_expired_rows_with_nothing_to_rebuild_from() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    let began = registry
        .begin_pause(&entry(sandbox_id, NODE_A))
        .await
        .expect("begin pause");
    registry
        .mark_local_only(&sandbox_id, began.generation)
        .await
        .expect("publish never landed");
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("resumed locally");
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::seconds(-1))])
        .await
        .expect("renew");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert_eq!(reclaimed.released, 0);
    assert_eq!(reclaimed.discarded, 1);
    assert!(registry
        .get(&sandbox_id)
        .await
        .expect("read back")
        .is_none());
}

/// The deadline has to come from the holder, not from the row: `metadata` is
/// whatever the sandbox looked like when it was paused, and a resume that set a
/// longer timeout only exists in what the holder reports. Deriving it from the
/// row would retire a sandbox that still had hours left.
#[tokio::test]
async fn a_renewal_moves_the_deadline_the_row_is_judged_against() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::seconds(-1))])
        .await
        .expect("first renewal: already past");
    // The sandbox's timeout is extended while it runs.
    registry
        .renew_lease(NODE_A, &[held_due(sandbox_id, TimeDelta::hours(1))])
        .await
        .expect("second renewal: an hour out");

    tokio::time::sleep(PAST_LEASE).await;

    let reclaimed = registry
        .reclaim_expired_holdings()
        .await
        .expect("reclaim expired");
    assert!(
        reclaimed.is_empty(),
        "an extended deadline must be what counts, got {reclaimed:?}"
    );
}
