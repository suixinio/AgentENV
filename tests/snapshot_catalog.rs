//! The central snapshot catalog client, against a real `SnapshotCatalog`
//! server.
//!
//! 🔴 Deliberately not a hand-written stub. Everything the client has to get
//! right here is a property of the *server*: which refusal a duplicate alias
//! produces, whether a `building` row is visible to a caller that never
//! mentioned `allow_any_status`, whether a commit without a `building` row is
//! refused, whether a `bytea` round-trips a `CommittedSnapshot` unchanged. A
//! stub written from the same reading of the proto that the client was written
//! from agrees with the client by construction and proves nothing.
//!
//! Run it against a scheduler with the catalog migration applied:
//!
//! ```sh
//! docker run -d --rm --name pg -e POSTGRES_PASSWORD=verify \
//!     -e POSTGRES_DB=aenv_registry -p 15501:5432 postgres:16-alpine
//! (cd services && go build -o /tmp/aenv-scheduler ./scheduler/cmd)
//! SCHEDULER_REGISTRY_DSN=postgres://postgres:verify@127.0.0.1:15501/aenv_registry \
//! SCHEDULER_REGISTRY_CLUSTER_ID=<uuid> SCHEDULER_REGISTRY_WRITE_ENABLED=true \
//!     /tmp/aenv-scheduler -config <config.json> &
//! AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT=http://127.0.0.1:19090 \
//! AENV_SNAPSHOT_CATALOG_TEST_CLUSTER_ID=<uuid> \
//! AENV_SNAPSHOT_CATALOG_TEST_REQUIRED=1 \
//!     cargo test -p agentenv --test snapshot_catalog
//! ```
//!
//! 🔴 The throwaway PostgreSQL in that recipe is not a convenience. The server
//! is whatever `AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT` names, these tests *write*
//! to it — snapshots, templates, aliases — and nothing here cleans up after
//! itself, so every row a run creates stays in that database under the cluster
//! id it was handed. One run pointed at the deployed dev-cluster scheduler left
//! 26 snapshot rows, 3 template rows and 10 alias rows behind in a database that
//! real sandboxes were using, and nothing in this file said it would.
//! `make test-snapshot-catalog` brings up a container PostgreSQL and a scheduler
//! of its own and tears both down afterwards; that is the reason to reach for it
//! rather than for an endpoint that already exists.
//!
//! 🔴 Without the endpoint every test here skips, and a skip in `go test`'s
//! default mode reports as `ok`. `AENV_SNAPSHOT_CATALOG_TEST_REQUIRED=1` turns
//! the missing dependency into a failure, which is what CI must set — the same
//! arrangement, for the same reason, as `SCHEDULER_REGISTRY_TEST_REQUIRED`.

use std::collections::HashMap;
use std::sync::Arc;

use agentenv::api::{snapshot_cursor_from_token, snapshot_next_token};
use agentenv::sandbox::FirecrackerSnapshotManifest;
use agentenv::snapshot::repository::backends::{CatalogReadScope, CentralSnapshotCatalog};
use agentenv::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit};
use agentenv::snapshot::repository::{RepositoryError, SnapshotCursor, SnapshotListFilter};
use agentenv::snapshot::{
    CommandContext, CommittedSnapshot, ManagedLayer, SnapshotAlias, SnapshotId,
    SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use agentenv::types::{ImageConfigs, SandboxResources};
use agentenv::virtualization::VirtualizationMode;
use uuid::Uuid;

/// The endpoint and cluster the tests run against, or a reason there are none.
struct Fixture {
    catalog: Arc<CentralSnapshotCatalog>,
}

/// Returns `None` when there is no server to talk to — unless the environment
/// says one was required, in which case it fails.
fn fixture() -> Option<Fixture> {
    let endpoint = std::env::var("AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let cluster = std::env::var("AENV_SNAPSHOT_CATALOG_TEST_CLUSTER_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());

    match (endpoint, cluster) {
        (Some(endpoint), Some(cluster)) => {
            let cluster_id = Uuid::parse_str(cluster.trim())
                .expect("AENV_SNAPSHOT_CATALOG_TEST_CLUSTER_ID should be a uuid");
            Some(Fixture {
                catalog: Arc::new(
                    CentralSnapshotCatalog::connect_lazy(
                        endpoint.trim(),
                        cluster_id,
                        "test-node-a".to_string(),
                    )
                    .expect("the endpoint should parse"),
                ),
            })
        }
        _ => {
            assert!(
                std::env::var("AENV_SNAPSHOT_CATALOG_TEST_REQUIRED").is_err(),
                "AENV_SNAPSHOT_CATALOG_TEST_REQUIRED is set but no catalog endpoint was given: \
                 set AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT and \
                 AENV_SNAPSHOT_CATALOG_TEST_CLUSTER_ID"
            );
            eprintln!(
                "skipping: no AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT, so nothing in this file ran"
            );
            None
        }
    }
}

macro_rules! catalog {
    () => {
        match fixture() {
            Some(fixture) => fixture.catalog,
            None => return,
        }
    };
}

fn committed(marker: &str) -> CommittedSnapshot {
    CommittedSnapshot {
        context: CommandContext::new(
            HashMap::from([("MARKER".to_string(), marker.to_string())]),
            "/workspace",
        ),
        startup: None,
        runtime_versions: SnapshotRuntimeVersions {
            kernel_version: "kernel-6.1".to_string(),
            firecracker_version: "1.7.0".to_string(),
            envd_version: "0.9.9".to_string(),
            tools_drive_version: "0.1.0".to_string(),
        },
        virtualization_mode: VirtualizationMode::default(),
        image_configs: ImageConfigs::new(),
        rootfs_layers: Vec::new(),
        attached_drives: Vec::new(),
        memory_layers: vec![ManagedLayer {
            digest: format!("sha256:{marker}"),
            size: 4096,
            uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
        }],
        disk_publications: Vec::new(),
        custom_extension_params: None,
    }
}

fn sandbox_commit(id: SnapshotId, alias: Option<&str>, marker: &str) -> SnapshotCommit {
    sandbox_commit_created_at(id, alias, marker, now_unix_ms())
}

/// The same commit, made to claim it was created at a stated instant.
///
/// 🔴 The backfill's whole job is to replay writes that are *older* than the
/// queue, so the tests that cover it need a commit whose creation time is not
/// the moment the test ran — otherwise "the replay stamped its own clock" and
/// "the replay carried the original" look identical.
fn sandbox_commit_created_at(
    id: SnapshotId,
    alias: Option<&str>,
    marker: &str,
    created_at_unix_ms: i64,
) -> SnapshotCommit {
    SnapshotCommit {
        id,
        alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias should parse")),
        source: SnapshotPublishSource::Sandbox {
            source_sandbox_id: "sbx-integration".to_string(),
        },
        resources: SandboxResources {
            cpu_count: 2,
            memory_mib: 512,
            disk_size_mib: 2048,
        },
        created_at_unix_ms: Some(created_at_unix_ms),
        committed: committed(marker),
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn template_record(id: SnapshotId, alias: Option<&str>) -> SnapshotRecord {
    SnapshotRecord::template_waiting(
        id,
        alias.map(|alias| SnapshotAlias::parse(alias).expect("alias should parse")),
        SandboxResources {
            cpu_count: 1,
            memory_mib: 256,
            disk_size_mib: 1024,
        },
    )
}

fn unique_alias(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::now_v7().simple())
}

/// The pause path end to end: two statements open and flip one row, and the
/// payload the row carries comes back exactly as it went in.
#[tokio::test]
async fn a_sandbox_snapshot_publishes_and_reads_back_with_its_payload_intact() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    let alias = unique_alias("pub");
    let commit = sandbox_commit(id.clone(), Some(&alias), "payload-round-trip");

    let published = catalog
        .publish_commit(commit.clone())
        .await
        .expect("publishing should work");

    assert_eq!(published.id, id);
    assert_eq!(published.resources.cpu_count, 2);
    assert!(matches!(
        published.source,
        SnapshotSource::Sandbox { ref source_sandbox_id } if source_sandbox_id == "sbx-integration"
    ));

    let read_back = catalog
        .get(&id.to_string())
        .await
        .expect("reading should work")
        .expect("a committed snapshot should be resolvable");
    let payload = read_back
        .committed
        .expect("a ready row must carry its payload");
    assert_eq!(
        payload.context.env_vars.get("MARKER").map(String::as_str),
        Some("payload-round-trip"),
        "the opaque payload must survive the bytea column unchanged"
    );
    assert_eq!(payload.memory_layers.len(), 1);
    assert_eq!(payload.memory_layers[0].size, 4096);

    assert_eq!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work"),
        Some(id)
    );
}

/// 🔴 The `allow_any_status` guard, checked against the server that enforces
/// it. A row that is still `building` is a snapshot whose bytes may still be
/// uploading, and a caller that says nothing about the field must not be handed
/// one. The control is the same read asking for it by name.
#[tokio::test]
async fn a_still_building_row_is_invisible_to_a_caller_that_did_not_ask_for_it() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    let alias = unique_alias("building");
    let mut opening = template_record(id.clone(), Some(&alias));
    opening.source = SnapshotSource::Sandbox {
        source_sandbox_id: "sbx-still-uploading".to_string(),
    };

    catalog
        .begin_snapshot(
            &opening,
            agentenv::snapshot::repository::backends::central::STATUS_BUILDING,
            false,
        )
        .await
        .expect("opening the row should work");

    assert!(
        catalog
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_none(),
        "the default read must not resolve a row whose bytes are still uploading"
    );
    assert!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work")
            .is_none(),
        "nor must the alias resolve to it"
    );
    assert!(
        catalog
            .list(SnapshotListFilter::matches_all())
            .await
            .expect("listing should work")
            .iter()
            .all(|record| record.id != id),
        "nor must it appear in a listing"
    );

    // The control: the one caller that exists to see these rows.
    let seen = catalog
        .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
        .await
        .expect("reading should work")
        .expect("the build-status read must see a building row");
    assert_eq!(seen.id, id);
    assert!(seen.committed.is_none());
}

/// P3. Two snapshots race for one name: one wins, the other is refused, and
/// the refusal names the holder.
#[tokio::test]
async fn an_alias_a_live_snapshot_holds_refuses_the_second_commit() {
    let catalog = catalog!();
    let alias = unique_alias("contested");
    let first = SnapshotId::generate();
    let second = SnapshotId::generate();

    catalog
        .publish_commit(sandbox_commit(first.clone(), Some(&alias), "first"))
        .await
        .expect("the first publish should take the name");

    let error = catalog
        .publish_commit(sandbox_commit(second.clone(), Some(&alias), "second"))
        .await
        .expect_err("the second publish must not also get the name");

    match error {
        RepositoryError::AliasConflict {
            alias: reported,
            existing,
            new_id,
        } => {
            assert_eq!(reported, alias);
            assert_eq!(existing, first);
            assert_eq!(new_id, second);
        }
        other => panic!("expected an alias conflict, got {other:?}"),
    }

    // 🔴 The whole write is refused, not committed without the name: a
    // published snapshot the user cannot reach by the name they asked for is
    // exactly the defect the unique index removes.
    assert!(
        catalog
            .get(&second.to_string())
            .await
            .expect("reading should work")
            .is_none(),
        "a refused alias must leave no committed row behind"
    );
    assert_eq!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work"),
        Some(first)
    );
}

/// P3, concurrently. The sequential version proves the unique index refuses a
/// name already committed; it does not prove the index is what decides a race,
/// because a check-then-write with a wide enough gap passes it every time.
///
/// 🔴 Both commits are in flight before either has finished. Exactly one may
/// win — two winners would mean the name is not unique, and no winner would
/// mean two callers can lock each other out of a name neither gets.
#[tokio::test]
async fn two_commits_racing_for_one_name_produce_exactly_one_winner() {
    let catalog = catalog!();
    let alias = unique_alias("raced");
    let first = SnapshotId::generate();
    let second = SnapshotId::generate();

    let one = Arc::clone(&catalog);
    let two = Arc::clone(&catalog);
    let alias_one = alias.clone();
    let alias_two = alias.clone();
    let id_one = first.clone();
    let id_two = second.clone();

    let (left, right) = tokio::join!(
        async move {
            one.publish_commit(sandbox_commit(id_one, Some(&alias_one), "left"))
                .await
        },
        async move {
            two.publish_commit(sandbox_commit(id_two, Some(&alias_two), "right"))
                .await
        }
    );

    let winners = [&left, &right]
        .iter()
        .filter(|outcome| outcome.is_ok())
        .count();
    assert_eq!(
        winners, 1,
        "exactly one commit may take the name; got left={left:?} right={right:?}"
    );

    let winner = match (&left, &right) {
        (Ok(record), _) => record.id.clone(),
        (_, Ok(record)) => record.id.clone(),
        _ => unreachable!("checked above"),
    };
    let loser = match (&left, &right) {
        (Err(error), _) => error,
        (_, Err(error)) => error,
        _ => unreachable!("checked above"),
    };
    assert!(
        matches!(loser, RepositoryError::AliasConflict { .. }),
        "the loser must be told the name is taken, not handed some other failure: {loser:?}"
    );
    assert_eq!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work"),
        Some(winner.clone())
    );

    // 🔴 And the loser committed nothing at all. A row that took the commit but
    // not the name is a snapshot the user cannot reach by the name they asked
    // for, which is the defect the index exists to remove.
    let loser_id = if winner == first { second } else { first };
    assert!(
        catalog
            .get(&loser_id.to_string())
            .await
            .expect("reading should work")
            .is_none(),
        "a refused alias must leave no committed row behind"
    );
}

/// The commit's fence, from the outside. A snapshot whose row nobody opened
/// cannot be flipped to `ready` — which is what makes a crash between the bytes
/// and the commit leave nothing resolvable behind.
#[tokio::test]
async fn a_commit_is_refused_when_the_row_is_not_building() {
    let catalog = catalog!();
    let id = SnapshotId::generate();

    // A row that is already `ready`: committing over it a second time finds a
    // status the fence refuses.
    catalog
        .publish_commit(sandbox_commit(id.clone(), None, "first"))
        .await
        .expect("the first commit should work");

    let error = catalog
        .publish_commit(sandbox_commit(id.clone(), None, "second"))
        .await
        .expect_err("a second commit must not flip a row that is already ready");
    assert!(
        format!("{error}").contains("refused"),
        "expected a refusal, got {error}"
    );

    // The control: the first payload is still what the row carries.
    let payload = catalog
        .get(&id.to_string())
        .await
        .expect("reading should work")
        .expect("the row should still be there")
        .committed
        .expect("and still carry a payload");
    assert_eq!(
        payload.context.env_vars.get("MARKER").map(String::as_str),
        Some("first"),
        "a refused commit must not have overwritten the payload"
    );
}

/// Templates: created `waiting`, failed with a reason, and the reason comes
/// back through the jsonb column without anything on the far side reading it.
#[tokio::test]
async fn a_template_row_is_created_waiting_and_can_be_failed_with_a_reason() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    let alias = unique_alias("tmpl");

    catalog
        .create(template_record(id.clone(), Some(&alias)))
        .await
        .expect("creating a template row should work");

    let waiting = catalog
        .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
        .await
        .expect("reading should work")
        .expect("the template row should be there");
    assert!(matches!(
        waiting.source,
        SnapshotSource::Template { ref build }
            if build.status == agentenv::snapshot::TemplateBuildStatus::Waiting
    ));

    catalog
        .mark_build_error(
            &id,
            TemplateBuildErrorReason::with_step("the build died", "RUN apt-get"),
        )
        .await
        .expect("failing the build should work");

    let failed = catalog
        .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
        .await
        .expect("reading should work")
        .expect("the template row should still be there");
    match failed.source {
        SnapshotSource::Template { build } => {
            assert_eq!(build.status, agentenv::snapshot::TemplateBuildStatus::Error);
            let reason = build.error_reason.expect("an error row must say why");
            assert_eq!(reason.message, "the build died");
            assert_eq!(reason.step.as_deref(), Some("RUN apt-get"));
        }
        other => panic!("expected a template row, got {other:?}"),
    }
}

/// Delete is idempotent by contract, and deleting takes the alias with it.
#[tokio::test]
async fn deleting_is_idempotent_and_frees_the_alias() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    let alias = unique_alias("deleted");

    let record = catalog
        .publish_commit(sandbox_commit(id.clone(), Some(&alias), "doomed"))
        .await
        .expect("publishing should work");

    catalog
        .delete_record(&record)
        .await
        .expect("deleting should work");
    catalog
        .delete_record(&record)
        .await
        .expect("deleting again is a success, not a refusal");

    assert!(catalog
        .get(&id.to_string())
        .await
        .expect("reading should work")
        .is_none());
    assert!(catalog
        .resolve_alias(&alias)
        .await
        .expect("resolving should work")
        .is_none());

    // The control: the freed name can be taken by something else.
    let successor = SnapshotId::generate();
    catalog
        .publish_commit(sandbox_commit(successor.clone(), Some(&alias), "successor"))
        .await
        .expect("the name must be free after the delete");
    assert_eq!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work"),
        Some(successor)
    );
}

/// A filter narrows a listing on the server rather than in this process.
#[tokio::test]
async fn listing_filters_by_source_sandbox_on_the_server() {
    let catalog = catalog!();
    let wanted_sandbox = format!("sbx-{}", Uuid::now_v7().simple());
    let mut wanted = Vec::new();
    for index in 0..3 {
        let id = SnapshotId::generate();
        let mut commit = sandbox_commit(id.clone(), None, &format!("listed-{index}"));
        commit.source = SnapshotPublishSource::Sandbox {
            source_sandbox_id: wanted_sandbox.clone(),
        };
        catalog
            .publish_commit(commit)
            .await
            .expect("publishing should work");
        wanted.push(id);
    }
    // A row that must not come back.
    catalog
        .publish_commit(sandbox_commit(SnapshotId::generate(), None, "other"))
        .await
        .expect("publishing should work");

    let listed = catalog
        .list(SnapshotListFilter {
            source_sandbox_id: Some(wanted_sandbox.clone()),
            ..SnapshotListFilter::default()
        })
        .await
        .expect("listing should work");

    let listed_ids: Vec<String> = listed.iter().map(|record| record.id.to_string()).collect();
    assert_eq!(
        listed_ids.len(),
        3,
        "expected exactly the three rows for this sandbox, got {listed_ids:?}"
    );
    for id in &wanted {
        assert!(listed_ids.contains(&id.to_string()));
    }
}

/// 🔴 The scope check, against the server that enforces it. A row belonging to
/// another cluster must not be served to this one — the scope travels in every
/// statement, and a request that merely asks for a different one is refused.
#[tokio::test]
async fn a_client_scoped_to_another_cluster_is_refused_rather_than_served() {
    let Some(fixture) = fixture() else { return };
    let endpoint = std::env::var("AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT").expect("checked above");
    let stranger = CentralSnapshotCatalog::connect_lazy(
        endpoint.trim(),
        Uuid::now_v7(),
        "test-node-a".to_string(),
    )
    .expect("the endpoint should parse");

    let id = SnapshotId::generate();
    fixture
        .catalog
        .publish_commit(sandbox_commit(id.clone(), None, "ours"))
        .await
        .expect("publishing should work");

    stranger
        .get(&id.to_string())
        .await
        .expect_err("a client in another cluster must not be served this cluster's rows");
}

/// 🔴 P7's first blocker, against the server that produced it. Every v3
/// template create failed the central catalog with `INVALID_ARGUMENT` while the
/// user was told **202**: the node sends `disk_size_mib = 0` because a v3
/// build request has no disk field and the real number is the built rootfs's
/// virtual size, and the store refused the row over it. The rule belongs to a
/// row somebody can launch, and a `waiting` template is not one.
///
/// A property of the *server* — the guard is in Go and the CHECK is in the
/// table — which is why it is tested here and not against a double.
#[tokio::test]
async fn a_template_that_does_not_know_its_disk_size_yet_is_accepted() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    let alias = unique_alias("nodisk");

    let mut record = SnapshotRecord::template_waiting(
        id.clone(),
        Some(SnapshotAlias::parse(&alias).expect("alias should parse")),
        SandboxResources {
            cpu_count: 2,
            memory_mib: 4096,
            disk_size_mib: 0,
        },
    );
    record.created_at_unix_ms = 1;
    record.updated_at_unix_ms = 1;

    catalog
        .create(record)
        .await
        .expect("a template has no disk size until it has been built");

    let stored = catalog
        .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
        .await
        .expect("reading should work")
        .expect("the row should be there");
    assert_eq!(stored.resources.cpu_count, 2);
    assert_eq!(stored.resources.memory_mib, 4096);
    assert_eq!(stored.resources.disk_size_mib, 0, "still unknown");

    // And the build fills it in — which is where the rule applies, at the one
    // statement that produces a row somebody can launch.
    //
    // Opened at `building` directly: the commit's fence wants that status and
    // the transition into it is build admission, which this batch does not
    // wire. Every other property under test is the same either way.
    let built = SnapshotId::generate();
    let mut building = template_record(built.clone(), None);
    building.resources.disk_size_mib = 0;
    if let SnapshotSource::Template { build } = &mut building.source {
        build.status = TemplateBuildStatus::Building;
    }
    catalog
        .create(building)
        .await
        .expect("a build in progress does not know its disk size either");

    let mut commit = sandbox_commit(built.clone(), None, "built");
    commit.source = SnapshotPublishSource::Template;
    commit.resources.disk_size_mib = 8192;
    catalog
        .publish_commit(commit)
        .await
        .expect("the built template should commit");

    assert_eq!(
        catalog
            .get(&built.to_string())
            .await
            .expect("reading should work")
            .expect("the row should be ready")
            .resources
            .disk_size_mib,
        8192,
        "the size the build produced is what a launchable row states"
    );
}

/// The control face, and the reason the constraint exists at all: a row that
/// can be launched has to say how big its disk is. Moving the rule must not
/// have removed it.
#[tokio::test]
async fn a_commit_still_has_to_state_a_disk_size() {
    let catalog = catalog!();
    let id = SnapshotId::generate();

    let mut record = template_record(id.clone(), None);
    record.resources.disk_size_mib = 0;
    catalog.create(record).await.expect("the row should open");

    let mut commit = sandbox_commit(id.clone(), None, "no-disk");
    commit.source = SnapshotPublishSource::Template;
    commit.resources.disk_size_mib = 0;
    let error = catalog
        .publish_commit(commit)
        .await
        .expect_err("a row nobody can launch must not be made launchable");

    // 🔴 And it must arrive as a permanent rejection, not as a transport
    // failure: the compensator retries transport failures forever.
    assert!(
        matches!(error, RepositoryError::InvalidRequest { .. }),
        "a rule the store stated must not look like an outage: {error:?}"
    );
}

/// The whole publish path through the repository seam, against the real
/// catalog: bytes to a POSIX artifact store, row to PostgreSQL, and the staged
/// value survives a serde round trip in between.
#[tokio::test]
async fn a_staged_snapshot_commits_into_the_central_catalog_after_a_round_trip() {
    let catalog = catalog!();
    let workspace = tempfile::TempDir::new().expect("tempdir should exist");
    let (_, _, manifest): (_, _, FirecrackerSnapshotManifest) =
        agentenv::snapshot::mock::write_mock_built_artifacts(workspace.path())
            .expect("mock artifacts should write");
    let _ = manifest;

    let id = SnapshotId::generate();
    let commit = sandbox_commit(id.clone(), None, "staged-then-committed");
    let encoded = serde_json::to_vec(&commit).expect("a commit should serialize");
    let decoded: SnapshotCommit =
        serde_json::from_slice(&encoded).expect("a commit should deserialize");

    let record = catalog
        .publish_commit(decoded)
        .await
        .expect("a round-tripped commit must still commit");
    assert_eq!(record.id, id);
}

// ─────────────────────────────────────────────────────────────────────────────
// The double write, against the same real catalog
// ─────────────────────────────────────────────────────────────────────────────

mod dual_write {
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    use super::*;
    use agentenv::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use agentenv::snapshot::repository::mirror::{
        CentralCatalogWrites, DualWriteCatalog, MirrorBacklog, MirrorDirection, MirrorTargets,
    };
    use agentenv::snapshot::repository::RepositoryResult;

    /// A real object-store catalog with a switch that takes it away.
    ///
    /// 🔴 Wraps the POSIX backend rather than replacing it, so the "healthy"
    /// half of every test below is a store that actually reads and writes
    /// files. A fake on both sides would let the double write agree with itself.
    struct BreakableCatalog {
        inner: Arc<dyn SnapshotCatalog>,
        broken: AtomicBool,
    }

    impl BreakableCatalog {
        fn new(inner: Arc<dyn SnapshotCatalog>) -> Self {
            Self {
                inner,
                broken: AtomicBool::new(false),
            }
        }

        fn break_it(&self) {
            self.broken.store(true, Ordering::SeqCst);
        }

        fn fix_it(&self) {
            self.broken.store(false, Ordering::SeqCst);
        }

        fn refuse<T>(&self) -> Option<RepositoryResult<T>> {
            self.broken.load(Ordering::SeqCst).then(|| {
                Err(RepositoryError::Backend {
                    message: "object storage is unreachable".to_string(),
                    source: None,
                })
            })
        }
    }

    #[async_trait]
    impl SnapshotCatalog for BreakableCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.create(record).await,
            }
        }

        async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.publish_commit(commit).await,
            }
        }

        async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.get(id_or_alias).await,
            }
        }

        async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.list(filter).await,
            }
        }

        async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.delete_record(record).await,
            }
        }

        async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.resolve_alias(alias).await,
            }
        }

        async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.try_start_build(id).await,
            }
        }

        async fn mark_build_error(
            &self,
            id: &SnapshotId,
            reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            match self.refuse() {
                Some(refusal) => refusal,
                None => self.inner.mark_build_error(id, reason).await,
            }
        }
    }

    struct Both {
        _workspace: tempfile::TempDir,
        central: Arc<CentralSnapshotCatalog>,
        object_store: Arc<BreakableCatalog>,
        backlog: Arc<MirrorBacklog>,
        dual: DualWriteCatalog,
    }

    /// Builds the double write over the real catalog and a real POSIX store.
    ///
    /// The POSIX backend builds its runtime resolver from the global config, so
    /// one has to exist. Idempotent — the first test through wins.
    async fn both(central: Arc<CentralSnapshotCatalog>) -> Both {
        agentenv::cfg::ConfigManager::init_global().expect("a config should load");
        let workspace = tempfile::TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: workspace.path().join("repository"),
            cache_root: Some(workspace.path().join("cache")),
            runtime_cache_root: Some(workspace.path().join("cache").join("runtime")),
        })
        .expect("the POSIX backend should build");
        let object_store = Arc::new(BreakableCatalog::new(backend.repository().catalog()));
        let backlog = MirrorBacklog::open(workspace.path().join("mirror"))
            .await
            .expect("the backlog should open");
        let dual = DualWriteCatalog::new(
            Arc::clone(&central) as Arc<dyn CentralCatalogWrites>,
            Arc::clone(&object_store) as Arc<dyn SnapshotCatalog>,
            Arc::clone(&backlog),
        );

        Both {
            _workspace: workspace,
            central,
            object_store,
            backlog,
            dual,
        }
    }

    impl Both {
        /// The two stores a repair pass may replay into.
        fn targets(&self) -> MirrorTargets {
            MirrorTargets::object_store(Arc::clone(&self.object_store) as Arc<dyn SnapshotCatalog>)
                .with_central(Arc::clone(&self.central) as Arc<dyn CentralCatalogWrites>)
        }
    }

    /// I1, and P7 for one row: one publish, two catalogs, the same snapshot in
    /// both — checked by reading each of them directly rather than through the
    /// wrapper that wrote them.
    #[tokio::test]
    async fn one_publish_lands_in_both_catalogs() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();
        let alias = unique_alias("dual");

        let record = both
            .dual
            .publish_commit(sandbox_commit(id.clone(), Some(&alias), "in-both"))
            .await
            .expect("the double write should succeed");
        assert_eq!(record.id, id);
        assert_eq!(both.backlog.lag(), 0, "nothing should be owed");

        let central_row = both
            .central
            .get(&id.to_string())
            .await
            .expect("reading the central catalog should work")
            .expect("the central catalog should have the row");
        let object_store_row = both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading object storage should work")
            .expect("object storage should have the row");

        assert_eq!(central_row.id, object_store_row.id);
        assert_eq!(
            central_row
                .committed
                .expect("central payload")
                .context
                .env_vars,
            object_store_row
                .committed
                .expect("object-store payload")
                .context
                .env_vars,
            "the same snapshot must be described the same way in both catalogs"
        );
        assert_eq!(
            both.object_store
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(id.clone())
        );
        assert_eq!(
            both.central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(id)
        );
    }

    /// 🔴 I2, as it now stands. A central catalog nobody can reach does **not**
    /// fail the write: the object store is written, the central write is
    /// recorded as owed, and the operation succeeds.
    ///
    /// It used to propagate, and what that bought was a pause whose scheduler
    /// was down failing at `commit_staged`, which rolled the publish back,
    /// which asked whether the artifacts were still owned, which was answered
    /// from the object store that had never been written — and deleted the
    /// bytes of the sandbox the user had just paused.
    ///
    /// What is still refused is a catalog that *answered* and said no; that is
    /// `an_alias_a_live_snapshot_holds_refuses_the_second_commit`, and the unit
    /// tests beside `DualWriteCatalog` cover every path's version of it.
    #[tokio::test]
    async fn an_unreachable_central_catalog_does_not_fail_the_write() {
        let Some(_) = fixture() else { return };
        // Nothing is listening here; the client is lazy, so this fails on use.
        let unreachable = Arc::new(
            CentralSnapshotCatalog::connect_lazy(
                "http://127.0.0.1:1",
                Uuid::now_v7(),
                "test-node-a".to_string(),
            )
            .expect("the endpoint should parse")
            .with_call_timeout(std::time::Duration::from_millis(500)),
        );
        let both = both(unreachable).await;
        let id = SnapshotId::generate();

        both.dual
            .publish_commit(sandbox_commit(id.clone(), None, "owed-to-the-catalog"))
            .await
            .expect("an unreachable central catalog must not fail a real publish");

        assert!(
            both.object_store
                .get(&id.to_string())
                .await
                .expect("reading should work")
                .is_some(),
            "the store that answers reads must still have been written"
        );
        assert_eq!(
            both.backlog.lag_toward(MirrorDirection::Central),
            1,
            "and the write the catalog missed must be owed to it"
        );
        assert_eq!(both.backlog.lag_toward(MirrorDirection::ObjectStore), 0);
    }

    /// 🔴 The debt is payable against the *real* catalog, which is what makes
    /// recording it instead of failing defensible. The replay re-runs both
    /// statements and the opening one answering `ALREADY_EXISTS` is the
    /// ordinary case — a property of the server, and the reason a stub would
    /// prove nothing here.
    #[tokio::test]
    async fn a_publish_owed_to_the_real_catalog_replays_into_it() {
        let central = catalog!();
        let unreachable = Arc::new(
            CentralSnapshotCatalog::connect_lazy(
                "http://127.0.0.1:1",
                Uuid::now_v7(),
                "test-node-a".to_string(),
            )
            .expect("the endpoint should parse")
            .with_call_timeout(std::time::Duration::from_millis(500)),
        );
        let both = both(unreachable).await;
        let id = SnapshotId::generate();
        let alias = unique_alias("owed");

        both.dual
            .publish_commit(sandbox_commit(id.clone(), Some(&alias), "replayed-later"))
            .await
            .expect("an unreachable catalog must not fail the publish");
        assert_eq!(both.backlog.lag_toward(MirrorDirection::Central), 1);
        assert!(
            central
                .get(&id.to_string())
                .await
                .expect("reading should work")
                .is_none(),
            "nothing reached the catalog yet"
        );

        let pass = both
            .backlog
            .drain_once(
                &MirrorTargets::object_store(
                    Arc::clone(&both.object_store) as Arc<dyn SnapshotCatalog>
                )
                .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
            )
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(both.backlog.lag_toward(MirrorDirection::Central), 0);
        let replayed = central
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .expect("the replay must have committed the row into the real catalog");
        assert_eq!(
            replayed
                .committed
                .expect("a ready row must carry its payload")
                .context
                .env_vars
                .get("MARKER")
                .map(String::as_str),
            Some("replayed-later"),
            "and it must carry the payload the original publish had"
        );
        assert_eq!(
            central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(id),
            "including the name it was published under"
        );
    }

    /// 🔴 I3 and P8. Object storage fails after the central catalog took the
    /// write. The publish still succeeds — making it fail would put publishing
    /// behind an AND of two systems — and the lag says exactly how far behind
    /// object storage now is.
    #[tokio::test]
    async fn an_object_store_failure_leaves_the_publish_successful_and_the_write_owed() {
        let central = catalog!();
        let both = both(central).await;
        both.object_store.break_it();

        let mut ids = Vec::new();
        for index in 0..3 {
            let id = SnapshotId::generate();
            both.dual
                .publish_commit(sandbox_commit(id.clone(), None, &format!("owed-{index}")))
                .await
                .expect("a broken mirror must not fail a real publish");
            ids.push(id);
        }

        assert_eq!(both.backlog.lag(), 3, "every failed mirror write is owed");
        for id in &ids {
            assert!(
                both.central
                    .get(&id.to_string())
                    .await
                    .expect("reading should work")
                    .is_some(),
                "the central catalog took every one of them"
            );
        }

        // 🔴 I4, and P8's control. Object storage comes back and the
        // compensator's pass clears the debt — the same switch that was
        // refused a moment ago is then allowed.
        both.object_store.fix_it();
        let pass = both
            .backlog
            .drain_once(&both.targets())
            .await
            .expect("the repair pass should run");
        assert_eq!(pass.repaired, 3);
        assert_eq!(both.backlog.lag(), 0);

        for id in &ids {
            assert!(
                both.object_store
                    .get(&id.to_string())
                    .await
                    .expect("reading should work")
                    .is_some(),
                "the repair must have put the row into object storage"
            );
        }
    }

    /// A template row is created in both, and failed in both.
    #[tokio::test]
    async fn creating_and_failing_a_template_reaches_both_catalogs() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();
        let alias = unique_alias("dual-tmpl");

        both.dual
            .create(template_record(id.clone(), Some(&alias)))
            .await
            .expect("creating should work");

        assert!(both
            .central
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("reading should work")
            .is_some());
        assert!(both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_some());

        both.dual
            .mark_build_error(&id, TemplateBuildErrorReason::new("no"))
            .await
            .expect("failing the build should work");

        let central_row = both
            .central
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("reading should work")
            .expect("the row should be there");
        assert!(matches!(
            central_row.source,
            SnapshotSource::Template { ref build }
                if build.status == agentenv::snapshot::TemplateBuildStatus::Error
        ));
        let object_store_row = both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .expect("the row should be there");
        assert!(matches!(
            object_store_row.source,
            SnapshotSource::Template { ref build }
                if build.status == agentenv::snapshot::TemplateBuildStatus::Error
        ));
    }

    /// Deleting removes the row from both.
    #[tokio::test]
    async fn deleting_reaches_both_catalogs() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();

        let record = both
            .dual
            .publish_commit(sandbox_commit(id.clone(), None, "doomed"))
            .await
            .expect("publishing should work");
        both.dual
            .delete_record(&record)
            .await
            .expect("deleting should work");

        assert!(both
            .central
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_none());
        assert!(both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_none());
    }

    /// 🔴 The batch's one known gap, asserted rather than left to be
    /// discovered. `try_start_build` is the catalog's build-admission
    /// transition, which is not wired here, so the write goes to object storage
    /// alone and the central row stays where the create left it.
    #[tokio::test]
    async fn starting_a_build_writes_object_storage_only() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();

        both.dual
            .create(template_record(id.clone(), None))
            .await
            .expect("creating should work");
        both.dual
            .try_start_build(&id)
            .await
            .expect("starting the build should work through object storage");

        let object_store_row = both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .expect("the row should be there");
        assert!(matches!(
            object_store_row.source,
            SnapshotSource::Template { ref build }
                if build.status == agentenv::snapshot::TemplateBuildStatus::Building
        ));

        let central_row = both
            .central
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("reading should work")
            .expect("the row should be there");
        assert!(
            matches!(
                central_row.source,
                SnapshotSource::Template { ref build }
                    if build.status == agentenv::snapshot::TemplateBuildStatus::Waiting
            ),
            "the central row is expected to stay behind here; if this starts \
             failing, build admission has been wired and the divergence \
             counter it feeds should go with it"
        );
        assert_eq!(
            both.backlog.lag(),
            0,
            "the gap is not an owed write: nothing the compensator can replay would close it"
        );
        assert_eq!(
            both.backlog.diverged_toward(MirrorDirection::Central),
            1,
            "🔴 but it is a disagreement, and something other than the lag has to say so — \
             a switch to reading PostgreSQL authorised on a lag of zero would make this \
             template vanish from the API"
        );
    }

    /// Reads come from object storage in this phase, and only from it.
    #[tokio::test]
    async fn reads_come_from_the_object_store() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();
        both.dual
            .publish_commit(sandbox_commit(id.clone(), None, "readable"))
            .await
            .expect("publishing should work");

        both.object_store.break_it();
        both.dual.get(&id.to_string()).await.expect_err(
            "a read must fail with object storage rather than fall back to the catalog",
        );
        both.dual
            .list(SnapshotListFilter::matches_all())
            .await
            .expect_err("so must a listing");
    }

    /// 🔴 P7 as it was actually measured: every template create answered **202**
    /// and PostgreSQL held **zero** rows, because the node sent a disk size the
    /// store refused. Through the double write, against the real server, with
    /// the fully-specified request the cluster probe used.
    #[tokio::test]
    async fn a_template_create_reaches_both_catalogs() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();
        let alias = unique_alias("tmpl");

        let record = SnapshotRecord::template_waiting(
            id.clone(),
            Some(SnapshotAlias::parse(&alias).expect("alias should parse")),
            SandboxResources {
                cpu_count: 2,
                memory_mib: 4096,
                disk_size_mib: 0,
            },
        );
        both.dual
            .create(record)
            .await
            .expect("creating a template must reach both catalogs");

        assert!(
            both.central
                .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
                .await
                .expect("reading should work")
                .is_some(),
            "the central catalog must hold the row the user was told was created"
        );
        assert!(both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_some());
        assert_eq!(
            both.backlog.lag_toward(MirrorDirection::Central),
            0,
            "nothing is owed, because nothing was refused"
        );
        assert_eq!(both.backlog.diverged_toward(MirrorDirection::Central), 0);
    }

    /// 🔴 P7's user-visible regression, as the invariant that was broken:
    /// twenty concurrent creates for one name answered **202** twenty times
    /// while three records existed. **As many callers told yes as there are
    /// records**, however the two stores resolve the race.
    ///
    /// 🔴 What this does *not* prove is that `create`'s alias carve-out works,
    /// and it is worth saying so: with a reachable central catalog the losers
    /// are refused by PostgreSQL's unique index before the object store is
    /// touched at all, so removing the carve-out leaves this test green. The
    /// case the carve-out is for is the one below — a name only the object
    /// store holds — and the unit test beside `DualWriteCatalog` covers the
    /// same guard against a double.
    #[tokio::test]
    async fn as_many_creates_succeed_as_there_are_records() {
        let central = catalog!();
        let both = Arc::new(both(central).await);
        let alias = unique_alias("contested");

        let mut racing = Vec::new();
        for _ in 0..8 {
            let both = Arc::clone(&both);
            let alias = alias.clone();
            let id = SnapshotId::generate();
            racing.push(tokio::spawn(async move {
                let record = SnapshotRecord::template_waiting(
                    id.clone(),
                    Some(SnapshotAlias::parse(&alias).expect("alias should parse")),
                    SandboxResources {
                        cpu_count: 1,
                        memory_mib: 256,
                        disk_size_mib: 0,
                    },
                );
                (id, both.dual.create(record).await.is_ok())
            }));
        }

        let mut winners = Vec::new();
        let mut losers = Vec::new();
        for task in racing {
            let (id, won) = task.await.expect("the create should not panic");
            if won {
                winners.push(id);
            } else {
                losers.push(id);
            }
        }

        assert_eq!(
            winners.len(),
            1,
            "exactly one caller may be told they hold this name"
        );
        assert_eq!(losers.len(), 7);
        assert_eq!(
            both.object_store
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(winners[0].clone()),
            "and the name must belong to the caller who was told so"
        );
        for id in &losers {
            assert!(
                both.central
                    .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
                    .await
                    .expect("reading should work")
                    .is_none(),
                "a create the caller was told failed must not be left in the central catalog"
            );
        }
    }

    /// 🔴 The alias hijack, in the exact state every alias is in the moment
    /// `write = "both"` is switched on: object storage holds the name and the
    /// central catalog has never heard of it.
    ///
    /// Measured on the cluster: the user got an error, PostgreSQL was left
    /// bound to the **new** snapshot and object storage still bound the old
    /// one — so the same name resolved to two different snapshots depending on
    /// which catalog answered, and moving reads to PostgreSQL would have
    /// silently changed what it meant.
    #[tokio::test]
    async fn an_alias_only_the_object_store_holds_is_not_hijacked() {
        let central = catalog!();
        let both = both(central).await;
        let alias = unique_alias("preflip");
        let seeded = SnapshotId::generate();
        let newcomer = SnapshotId::generate();

        // The pre-flip state: object storage alone knows this name.
        both.object_store
            .publish_commit(sandbox_commit(seeded.clone(), Some(&alias), "seed"))
            .await
            .expect("seeding the object store should work");
        assert_eq!(
            both.central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            None,
            "this test is only meaningful while the central catalog has never seen the name"
        );

        both.dual
            .publish_commit(sandbox_commit(newcomer.clone(), Some(&alias), "hijacker"))
            .await
            .expect_err("a name the object store holds must not be handed to somebody else");

        assert_eq!(
            both.object_store
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(seeded.clone()),
            "the store that answers reads must still bind the name it bound before"
        );
        assert_eq!(
            both.central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            None,
            "and the other one must not have been bound to the snapshot that lost"
        );
        assert!(
            both.central
                .get_scoped(&newcomer.to_string(), CatalogReadScope::AnyStatus)
                .await
                .expect("reading should work")
                .is_none(),
            "the row the failed publish opened must not be left behind either"
        );
    }

    /// 🔴 `create`'s alias carve-out, in the state that needs it: the object
    /// store holds the name and the central catalog has never heard of it,
    /// which is every alias at the moment `write = "both"` is switched on. The
    /// central catalog takes the row, the object store refuses the name, and
    /// without the carve-out the caller is told **202** over a template no
    /// catalog holds under the name they asked for.
    #[tokio::test]
    async fn a_name_only_the_object_store_holds_refuses_a_create() {
        let central = catalog!();
        let both = both(central).await;
        let alias = unique_alias("held");
        let seeded = SnapshotId::generate();
        let newcomer = SnapshotId::generate();

        both.object_store
            .create(template_record(seeded.clone(), Some(&alias)))
            .await
            .expect("seeding the object store should work");
        assert_eq!(
            both.central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            None,
            "this test is only meaningful while the central catalog has never seen the name"
        );

        let error = both
            .dual
            .create(template_record(newcomer.clone(), Some(&alias)))
            .await
            .expect_err("a name the object store holds must reach the caller");
        assert!(
            matches!(error, RepositoryError::AliasConflict { .. }),
            "expected an alias conflict, got {error:?}"
        );

        assert_eq!(
            both.object_store
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(seeded),
            "the store that answers reads keeps the name it already bound"
        );
        assert!(
            both.central
                .get_scoped(&newcomer.to_string(), CatalogReadScope::AnyStatus)
                .await
                .expect("reading should work")
                .is_none(),
            "and the row the refused create opened must not be left behind"
        );
    }

    /// 🔴 And once the history is queued, the same collision is refused by the
    /// central catalog's own unique index rather than by an undo — which is the
    /// difference between a race that is handled and one that cannot happen.
    /// 🔴 B-1, against the real catalog. A backfilled snapshot keeps the
    /// creation time object storage already held for it.
    ///
    /// Measured on the cluster: every one of the 32 backfilled rows recorded
    /// the instant the *backfill* ran. `created_at_ms` is what the listing
    /// orders by and what `createdAt` is served from, so PostgreSQL returned
    /// them in a completely different order from object storage and would have
    /// told every caller the wrong date — while `lag == 0` reported agreement.
    #[tokio::test]
    async fn a_backfilled_snapshot_keeps_the_creation_time_object_storage_held() {
        let central = catalog!();
        let both = both(central).await;
        let seeded = SnapshotId::generate();
        // Far enough in the past that no clock read could produce it by
        // accident: 2023-07-22.
        let created_at = 1_690_000_000_000;

        both.object_store
            .publish_commit(sandbox_commit_created_at(
                seeded.clone(),
                None,
                "seed",
                created_at,
            ))
            .await
            .expect("seeding the object store should work");
        assert_eq!(
            both.object_store
                .get(&seeded.to_string())
                .await
                .expect("reading should work")
                .expect("the row should be there")
                .created_at_unix_ms,
            created_at,
            "the object store has to hold the stated instant for the rest of this to mean \
             anything"
        );

        both.backlog
            .queue_history_toward_central(both.object_store.as_ref() as &dyn SnapshotCatalog)
            .await
            .expect("the history should queue");
        let pass = both
            .backlog
            .drain_once(&both.targets())
            .await
            .expect("the pass should run");
        assert_eq!(pass.repaired, 1);

        let row = both
            .central
            .get_scoped(&seeded.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("reading should work")
            .expect("the backfilled row should be there");
        assert_eq!(
            row.created_at_unix_ms, created_at,
            "a replayed publish must record the snapshot's creation time, not the replay's"
        );
    }

    /// 🔴 B-1's second half, against the real catalog: `lag == 0` now means the
    /// two catalogs' rows *say the same thing*, not merely that both stores
    /// took the write.
    ///
    /// The central catalog is given the row first, with a creation time five
    /// seconds off what the create about to be replayed carries. Object storage
    /// then takes that create exactly as it always did — and the entry is still
    /// counted as debt, because the two rows disagree.
    #[tokio::test]
    async fn a_row_the_store_took_but_that_disagrees_is_still_counted() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();
        let record = template_record(id.clone(), None);

        let mut drifted = record.clone();
        drifted.created_at_unix_ms -= 5_000;
        both.central
            .begin_snapshot(&drifted, "waiting", true)
            .await
            .expect("opening the central row should work");

        // Object storage is away, so the create is owed to it and to nothing
        // else — the central catalog answers `AlreadyExists`, which is settled.
        both.object_store.break_it();
        both.dual
            .create(record)
            .await
            .expect("an object store that is away must not fail the write");
        assert_eq!(both.backlog.lag_toward(MirrorDirection::ObjectStore), 1);
        both.object_store.fix_it();

        let pass = both
            .backlog
            .drain_once(&both.targets())
            .await
            .expect("the pass should run");

        assert!(
            both.object_store
                .get(&id.to_string())
                .await
                .expect("reading should work")
                .is_some(),
            "the write itself still lands"
        );
        assert_eq!(pass.repaired, 0, "but the two catalogs do not agree");
        assert_eq!(
            both.backlog.lag_toward(MirrorDirection::ObjectStore),
            1,
            "and the lag must not reach zero over rows that say different things"
        );
    }

    /// 🔴 B-2, against the real catalog. A divergence must not outlive the
    /// snapshot it is about.
    ///
    /// `clear_divergences` runs on delete, and the gateway routes the delete —
    /// measured as 13 deletes split 6/8 across two nodes, leaving one node's
    /// `mirror_diverged{central}` pinned at 1 over a snapshot that no longer
    /// existed anywhere, with no API call able to clear it. Here the delete
    /// happens the way another node's would: straight against both stores,
    /// without this node's `delete_record` ever running.
    #[tokio::test]
    async fn a_divergence_does_not_outlive_the_snapshot_when_another_node_deletes_it() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();

        both.dual
            .create(template_record(id.clone(), None))
            .await
            .expect("creating should work");
        both.dual
            .try_start_build(&id)
            .await
            .expect("starting the build should work through object storage");
        assert_eq!(
            both.backlog.diverged_toward(MirrorDirection::Central),
            1,
            "build admission is not wired, so this records a divergence"
        );

        let record = both
            .object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .expect("the row should be there");
        both.central
            .delete_snapshot(&id.to_string(), 1)
            .await
            .expect("deleting from the central catalog should work");
        both.object_store
            .delete_record(&record)
            .await
            .expect("deleting from object storage should work");

        let sweep = both
            .backlog
            .retire_settled_divergences(&both.targets())
            .await
            .expect("the sweep should run");
        assert_eq!(sweep.retired, 1);
        assert_eq!(
            both.backlog.diverged_toward(MirrorDirection::Central),
            0,
            "nothing is left for the two catalogs to disagree about"
        );
    }

    /// The control, and the one that keeps the fix above from being a way to
    /// clear a number by forgetting it: while either catalog still holds the
    /// snapshot, the disagreement is still real and the record stays.
    #[tokio::test]
    async fn a_divergence_about_a_live_snapshot_survives_the_sweep() {
        let central = catalog!();
        let both = both(central).await;
        let id = SnapshotId::generate();

        both.dual
            .create(template_record(id.clone(), None))
            .await
            .expect("creating should work");
        both.dual
            .try_start_build(&id)
            .await
            .expect("starting the build should work through object storage");

        let sweep = both
            .backlog
            .retire_settled_divergences(&both.targets())
            .await
            .expect("the sweep should run");
        assert_eq!(sweep.retired, 0);
        assert_eq!(sweep.kept, 1);
        assert_eq!(both.backlog.diverged_toward(MirrorDirection::Central), 1);
    }

    #[tokio::test]
    async fn a_queued_history_makes_the_hijack_impossible_rather_than_undone() {
        let central = catalog!();
        let both = both(central).await;
        let alias = unique_alias("backfilled");
        let seeded = SnapshotId::generate();

        both.object_store
            .publish_commit(sandbox_commit(seeded.clone(), Some(&alias), "seed"))
            .await
            .expect("seeding the object store should work");

        let queued = both
            .backlog
            .queue_history_toward_central(both.object_store.as_ref()
                as &dyn agentenv::snapshot::repository::interfaces::SnapshotCatalog)
            .await
            .expect("the history should queue");
        assert_eq!(queued, 1, "one snapshot older than the double write");

        let pass = both
            .backlog
            .drain_once(&both.targets())
            .await
            .expect("the pass should run");
        assert_eq!(pass.repaired, 1);
        assert_eq!(both.backlog.lag_toward(MirrorDirection::Central), 0);
        assert_eq!(
            both.central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(seeded),
            "the backfill carries the name across, not just the row"
        );

        // Now the central catalog refuses it first, before the object store is
        // ever asked — and the caller is told the same thing either way.
        let error = both
            .dual
            .publish_commit(sandbox_commit(
                SnapshotId::generate(),
                Some(&alias),
                "hijacker",
            ))
            .await
            .expect_err("a name the central catalog now holds must fail the publish");
        assert!(
            matches!(error, RepositoryError::AliasConflict { .. }),
            "a lost race for a name is a conflict on both sides: {error:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The seam, consumed across a process boundary
// ─────────────────────────────────────────────────────────────────────────────

mod bytes_then_commit {
    use super::*;

    use agentenv::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use agentenv::snapshot::repository::mirror::{
        CentralCatalogWrites, DualWriteCatalog, MirrorBacklog,
    };
    use agentenv::snapshot::repository::{SnapshotRepository, StagedSnapshot};
    use agentenv::snapshot::{SnapshotManager, SnapshotPublishMetadata};

    /// 🔴 The whole point of the split, end to end and across a real process
    /// boundary.
    ///
    /// `stage` writes bytes to this machine's disk and announces nothing. The
    /// value it hands back is written down, read back, and committed — and the
    /// commit travels over gRPC to a different process, which is the property
    /// `--role api` will depend on and the reason the value may not carry a
    /// path, a handle, or a manifest.
    ///
    /// The controls are the two reads between the two calls: with the bytes on
    /// disk and no commit, neither of them finds the snapshot.
    #[tokio::test]
    async fn bytes_land_here_and_the_commit_happens_in_another_process() {
        let central = catalog!();
        agentenv::cfg::ConfigManager::init_global().expect("a config should load");

        let workspace = tempfile::TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: workspace.path().join("repository"),
            cache_root: Some(workspace.path().join("cache")),
            runtime_cache_root: Some(workspace.path().join("cache").join("runtime")),
        })
        .expect("the POSIX backend should build");
        let object_store = backend.repository().catalog();
        let backlog = MirrorBacklog::open(workspace.path().join("mirror"))
            .await
            .expect("the backlog should open");
        let dual = Arc::new(DualWriteCatalog::new(
            Arc::clone(&central) as Arc<dyn CentralCatalogWrites>,
            Arc::clone(&object_store),
            backlog,
        ));
        let manager = SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::on_node(
                dual,
                backend.repository().artifacts(),
                "test-node-a".to_string(),
            )),
            backend.runtime_resolver(),
            None,
        );

        let artifacts = tempfile::TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            agentenv::snapshot::mock::write_mock_built_artifacts(artifacts.path())
                .expect("mock artifacts should write");

        let id = SnapshotId::generate();
        let alias = unique_alias("seam");
        let metadata = SnapshotPublishMetadata {
            id: id.clone(),
            alias: Some(SnapshotAlias::parse(&alias).expect("alias should parse")),
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx-seam".to_string(),
            },
            context: CommandContext::new(HashMap::new(), "/workspace"),
            startup: None,
            resources: SandboxResources {
                cpu_count: 2,
                memory_mib: 512,
                disk_size_mib: 2048,
            },
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel-6.1".to_string(),
                firecracker_version: "1.7.0".to_string(),
                envd_version: "0.9.9".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::default(),
            image_configs: ImageConfigs::new(),
            custom_extension_params: None,
        };

        let handle = manager
            .stage(metadata, manifest, None)
            .await
            .expect("staging should work");
        assert_eq!(handle.staged().origin_node_id, "test-node-a");

        // The bytes are here.
        assert!(workspace
            .path()
            .join("repository")
            .join("snapshots")
            .join(id.to_string())
            .join("vm_state.bin")
            .exists());
        // And nobody can find them, in either catalog.
        assert!(object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_none());
        assert!(central
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("reading should work")
            .is_none());

        // 🔴 Down the wire and back before it is used.
        let staged_at_unix_ms = handle.staged().staged_at_unix_ms;
        let encoded = serde_json::to_vec(handle.staged()).expect("the staged value should encode");
        drop(handle);
        let decoded: StagedSnapshot =
            serde_json::from_slice(&encoded).expect("the staged value should decode");

        // 🔴 A gap the clock can see. Without one, staging and committing land
        // in the same millisecond and "the row records the staged instant" and
        // "the row records whenever the flip ran" are the same number — which
        // is a test that cannot fail, not a test that passes.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            now_unix_ms() > staged_at_unix_ms,
            "the two instants have to be distinguishable for the assertions below to mean anything"
        );

        let record = manager
            .commit_staged(decoded)
            .await
            .expect("a round-tripped staged snapshot must commit");
        assert_eq!(record.id, id);

        // 🔴 One instant, decided where the bytes were staged, and the same one
        // in both catalogs. Left to each store's own clock the two rows differ
        // by an RPC's latency on every publish — and a commit that crossed a
        // process boundary would record whenever the *other* process got round
        // to it, which is the failure the backfill made visible at scale.
        assert_eq!(
            record.created_at_unix_ms, staged_at_unix_ms,
            "the row must record when the snapshot was staged, not when the flip ran"
        );
        assert_eq!(
            central
                .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
                .await
                .expect("reading should work")
                .expect("the row should be there")
                .created_at_unix_ms,
            staged_at_unix_ms,
            "and the catalog in the other process must record the same instant"
        );

        // Both catalogs now have it — the second one in a different process.
        assert!(object_store
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_some());
        let remote = central
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .expect("the row committed in the other process should be there");
        assert_eq!(remote.resources.cpu_count, 2);
        assert_eq!(
            central
                .resolve_alias(&alias)
                .await
                .expect("resolving should work"),
            Some(id)
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Keyset pagination, pushed into the server
// ─────────────────────────────────────────────────────────────────────────────

/// Publishes `count` snapshots for one source sandbox, in `groups` of rows
/// sharing a creation instant.
///
/// 🔴 The shared instants are the point. Ordering is `created_at_ms DESC, id
/// ASC`, so a cursor is only correct if it breaks ties by id — and a fixture
/// whose rows all have distinct timestamps cannot tell a cursor that does from
/// one that stops at the timestamp. Every page boundary below lands inside a
/// tie.
async fn publish_a_page_fixture(
    catalog: &CentralSnapshotCatalog,
    sandbox_id: &str,
    count: usize,
    group: usize,
) -> Vec<SnapshotId> {
    let base = now_unix_ms();
    let mut ids = Vec::with_capacity(count);
    for n in 0..count {
        let id = SnapshotId::generate();
        let mut commit =
            sandbox_commit_created_at(id.clone(), None, "paging", base - (n / group) as i64);
        commit.source = SnapshotPublishSource::Sandbox {
            source_sandbox_id: sandbox_id.to_string(),
        };
        catalog
            .publish_commit(commit)
            .await
            .expect("publishing should work");
        ids.push(id);
    }
    ids
}

fn page_filter(sandbox_id: &str, limit: u32, cursor: Option<SnapshotCursor>) -> SnapshotListFilter {
    SnapshotListFilter {
        source_sandbox_id: Some(sandbox_id.to_string()),
        ..SnapshotListFilter::default()
    }
    .paginated(Some(limit), cursor)
}

/// 🔴 P2, against the real keyset: walk a catalog to the end in pages of three
/// and get every row exactly once, in the listing's order.
#[tokio::test]
async fn walking_the_servers_cursor_returns_every_row_exactly_once() {
    let catalog = catalog!();
    let sandbox_id = unique_alias("sbx-page");
    let published = publish_a_page_fixture(&catalog, &sandbox_id, 10, 3).await;

    let mut seen: Vec<(i64, SnapshotId)> = Vec::new();
    let mut cursor: Option<SnapshotCursor> = None;
    for _ in 0..published.len() + 1 {
        let page = catalog
            .list_page(page_filter(&sandbox_id, 3, cursor.clone()))
            .await
            .expect("the listing should work");
        assert!(page.items.len() <= 3, "the server honoured the page size");
        seen.extend(
            page.items
                .iter()
                .map(|row| (row.created_at_unix_ms, row.id.clone())),
        );
        match page.next {
            None => break,
            Some(next) => cursor = Some(next),
        }
    }

    let ids: Vec<SnapshotId> = seen.iter().map(|(_, id)| id.clone()).collect();
    assert_eq!(ids.len(), published.len(), "every row came back");
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), published.len(), "and none of them twice");
    for id in &published {
        assert!(ids.contains(id), "{id} was published and never listed");
    }

    // 🔴 And in the order the index was built for: newest first, ties broken by
    // ascending id. Stated as the order the walk *produced*, sorted
    // independently, so a cursor that stopped at the timestamp fails here —
    // the fixture puts three rows in every instant precisely so that it can.
    let mut expected = seen.clone();
    expected.sort_by(|(a_ms, a_id), (b_ms, b_id)| b_ms.cmp(a_ms).then_with(|| a_id.cmp(b_id)));
    assert_eq!(
        seen, expected,
        "the walk must come back in `created_at_ms DESC, id ASC`"
    );
    assert!(
        seen.windows(2).any(|pair| pair[0].0 == pair[1].0),
        "the fixture must contain rows sharing an instant, or the tie-break is untested"
    );
}

/// 🔴 The whole batch, in one assertion: the page size reaches the query.
///
/// Before this, a listing was read whole and sliced afterwards, so a request
/// for one row cost what a request for a hundred did — measured on the dev
/// cluster as `1 + 32` object-store requests either way. The server answering
/// with one row and a cursor is what says the limit is no longer decoration.
#[tokio::test]
async fn a_page_of_one_is_answered_with_one_row_and_a_cursor() {
    let catalog = catalog!();
    let sandbox_id = unique_alias("sbx-limit");
    publish_a_page_fixture(&catalog, &sandbox_id, 4, 2).await;

    let page = catalog
        .list_page(page_filter(&sandbox_id, 1, None))
        .await
        .expect("the listing should work");

    assert_eq!(page.items.len(), 1);
    assert!(
        page.next.is_some(),
        "three more rows match, so there is another page"
    );

    let whole = catalog
        .list(SnapshotListFilter {
            source_sandbox_id: Some(sandbox_id.clone()),
            ..SnapshotListFilter::default()
        })
        .await
        .expect("the unbounded listing should work");
    assert_eq!(
        whole.len(),
        4,
        "and the unbounded listing still sees all of them"
    );
}

/// 🔴 The public token, carried through the server and back.
///
/// This is the test that catches the failure mode with no error message: the
/// public cursor orders ids by their *text* form, PostgreSQL is asked to
/// compare `s.id::text`, and the node compares `SnapshotId`. If those three ever
/// disagreed, a page boundary would silently skip a row. Rendering the server's
/// cursor into the public token, parsing it back, and asking the server to
/// continue from it exercises all three in one round trip.
#[tokio::test]
async fn the_public_token_round_trips_through_the_server_without_losing_a_row() {
    let catalog = catalog!();
    let sandbox_id = unique_alias("sbx-token");
    let published = publish_a_page_fixture(&catalog, &sandbox_id, 9, 3).await;

    let mut seen: Vec<SnapshotId> = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..published.len() + 1 {
        let cursor = match token.as_deref() {
            Some(token) => Some(
                snapshot_cursor_from_token(token).expect("the token this service minted parses"),
            ),
            None => None,
        };
        let page = catalog
            .list_page(page_filter(&sandbox_id, 2, cursor))
            .await
            .expect("the listing should work");
        seen.extend(page.items.iter().map(|row| row.id.clone()));
        match page.next.as_ref() {
            None => break,
            // 🔴 Through the public rendering every time, not around it.
            Some(next) => token = Some(snapshot_next_token(next)),
        }
    }

    assert_eq!(seen.len(), published.len(), "every row survived the token");
    let unique: std::collections::HashSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), published.len(), "and none of them twice");
}

/// 🔴 §5.3, on the paged read: the resolvable predicate is in the query, and a
/// caller that says nothing about status gets the reading that cannot start a
/// half-written VM.
///
/// `allow_any_status`'s zero value is the safe one on purpose — a client that
/// forgets it must not be handed a snapshot whose bytes are still uploading —
/// and this is the paged half of that, which did not exist before.
#[tokio::test]
async fn a_still_building_row_is_absent_from_a_page_that_did_not_ask_for_it() {
    let catalog = catalog!();
    let sandbox_id = unique_alias("sbx-scope");
    let building = SnapshotId::generate();
    let mut opening = SnapshotRecord::template_waiting(
        building.clone(),
        None,
        SandboxResources {
            cpu_count: 1,
            memory_mib: 256,
            disk_size_mib: 0,
        },
    );
    opening.source = SnapshotSource::Sandbox {
        source_sandbox_id: sandbox_id.clone(),
    };
    catalog
        .begin_snapshot(
            &opening,
            agentenv::snapshot::repository::backends::central::STATUS_BUILDING,
            false,
        )
        .await
        .expect("opening the row should work");

    let resolvable = catalog
        .list_page(page_filter(&sandbox_id, 10, None))
        .await
        .expect("the listing should work");
    assert!(
        !resolvable.items.iter().any(|row| row.id == building),
        "a row whose bytes are still uploading must not appear in a resolvable page"
    );

    // The control: the endpoint that exists to look at builds does see it, and
    // it is the same query with the one flag set.
    let any = catalog
        .list_page_scoped(
            page_filter(&sandbox_id, 10, None),
            CatalogReadScope::AnyStatus,
        )
        .await
        .expect("the listing should work");
    assert!(
        any.items.iter().any(|row| row.id == building),
        "asking for any status must find it, or this test proves nothing"
    );
}

/// A page size the caller did not state is the server's default, not "all of
/// them" — and the node's idea of that default has to be the server's, or an
/// unbounded request means two different things either side of the read switch.
#[tokio::test]
async fn a_page_with_no_stated_limit_is_bounded_by_the_server() {
    let catalog = catalog!();
    let sandbox_id = unique_alias("sbx-default");
    publish_a_page_fixture(&catalog, &sandbox_id, 3, 1).await;

    let page = catalog
        .list_page(SnapshotListFilter {
            source_sandbox_id: Some(sandbox_id.clone()),
            ..SnapshotListFilter::default()
        })
        .await
        .expect("the listing should work");

    assert_eq!(page.items.len(), 3);
    assert!(
        page.next.is_none(),
        "three rows fit inside the default page"
    );
}

/// 🔴 The read-side gate's census counts rows no resolving query can see.
///
/// Every other read this node makes of the central catalog is at the resolvable
/// scope, and that is right: it is what stops a snapshot whose bytes are still
/// uploading from starting a VM. Here it would be wrong in a way that fails
/// *closed on healthy clusters* — a template sitting at `waiting` is a row
/// object storage holds and counts, so hiding it on this side alone would make
/// the comparison refuse every cluster that has ever built a template. A gate
/// that refuses healthy clusters gets turned off, which is the same as not
/// having one.
#[tokio::test]
async fn the_read_side_gates_census_counts_rows_the_resolvable_reading_hides() {
    use agentenv::snapshot::repository::mirror::CatalogCensus;

    let catalog = catalog!();
    let waiting = SnapshotId::generate();
    catalog
        .create(template_record(waiting.clone(), None))
        .await
        .expect("creating the template row should work");

    let counted = catalog
        .every_snapshot_id()
        .await
        .expect("the census should work");
    assert!(
        counted.contains(&waiting),
        "the census must count a template that has not been built yet"
    );

    // The control: the reading everything else uses does not see it, so the
    // scope is doing the work rather than the row happening to be visible.
    let resolvable = catalog
        .list(SnapshotListFilter::matches_all())
        .await
        .expect("the resolvable listing should work");
    assert!(
        !resolvable.iter().any(|row| row.id == waiting),
        "a waiting template must be invisible to the resolvable reading"
    );
}

/// 🔴 The unbounded listing walks past its first page.
///
/// `list_scoped` is what the read-side gate's census and the mirror's history
/// backfill both read, and both are *counting everything*. A listing that
/// stopped at one page would make the gate compare a truncated central catalog
/// against a whole object store — refusing healthy clusters — and would make
/// the backfill queue only the first page of a history it reported as done.
/// Neither would look like a bug from the outside.
///
/// The fixture is deliberately larger than the client's internal page size,
/// which is the only size at which the difference exists.
#[tokio::test]
async fn the_unbounded_listing_walks_past_its_first_page() {
    let catalog = catalog!();
    // One more than the 200-row page the client asks the server for.
    const ROWS: usize = 201;
    let prefix = unique_alias("drain");

    for n in 0..ROWS {
        catalog
            .create(template_record(
                SnapshotId::generate(),
                Some(&format!("{prefix}-{n:04}")),
            ))
            .await
            .expect("creating the template row should work");
    }

    let listed = catalog
        .list_scoped(
            SnapshotListFilter {
                alias_prefix: Some(prefix.clone()),
                ..SnapshotListFilter::default()
            },
            CatalogReadScope::AnyStatus,
        )
        .await
        .expect("the unbounded listing should work");

    assert_eq!(
        listed.len(),
        ROWS,
        "the listing stopped early; a walk that ends at its first page reports a smaller \
         catalog than there is"
    );
}
