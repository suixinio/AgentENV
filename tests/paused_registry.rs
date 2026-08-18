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

use chrono::Utc;
use uuid::Uuid;

use agentenv::orchestrator::{
    PausedRegistryError, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
    PostgresPausedSandboxRegistry, ResumeClaim, SandboxMetadata,
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

fn entry(sandbox_id: SandboxId, origin: &str) -> PausedSandboxEntry {
    PausedSandboxEntry {
        sandbox_id,
        cluster_id: Uuid::nil(),
        state: PausedRegistryState::Publishing,
        generation: 0,
        origin_node_id: origin.to_string(),
        claimed_by_node_id: None,
        snapshot_id: None,
        metadata: SandboxMetadata::default(),
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
    let ResumeClaim::Claimed(claimed) = claim else {
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

/// The other half: the lease has to actually expire, or losing a node would
/// strand its sandboxes forever. This is the whole point of the mechanism —
/// a node that stops renewing has, by definition, let its sandboxes go.
#[tokio::test]
async fn a_holder_that_stops_renewing_loses_the_sandbox() {
    let dsn = require_db!();
    let registry = registry(&dsn, Uuid::new_v4()).await;
    let sandbox_id = SandboxId::new();

    pause_and_publish(&registry, sandbox_id, NODE_A).await;
    registry
        .mark_running(&sandbox_id, NODE_A)
        .await
        .expect("mark running");

    tokio::time::sleep(PAST_LEASE).await;

    let claim = registry
        .claim_for_resume(&sandbox_id, NODE_B)
        .await
        .expect("claim");
    assert!(
        matches!(claim, ResumeClaim::Claimed(_)),
        "a lapsed lease must let another node recover the sandbox"
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
            .renew_lease(NODE_B, &[sandbox_id])
            .await
            .expect("renew as a stranger"),
        0,
        "a node must not be able to renew a lease on a sandbox it does not hold"
    );
    assert_eq!(
        registry
            .renew_lease(NODE_A, &[sandbox_id])
            .await
            .expect("renew as the holder"),
        1
    );

    // Renewed, so the takeover that would otherwise succeed by now does not.
    tokio::time::sleep(Duration::from_millis(600)).await;
    registry
        .renew_lease(NODE_A, &[sandbox_id])
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
    assert!(matches!(claim, ResumeClaim::Claimed(_)));
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
    let ResumeClaim::Claimed(_) = registry
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
    let ResumeClaim::Claimed(claimed) = registry
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
