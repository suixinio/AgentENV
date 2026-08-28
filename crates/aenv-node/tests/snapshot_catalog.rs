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

use aenv_node::api::{snapshot_cursor_from_token, snapshot_next_token};
use aenv_node::sandbox::FirecrackerSnapshotManifest;
use aenv_node::snapshot::repository::backends::CentralSnapshotCatalog;
use aenv_node::snapshot::repository::interfaces::CatalogReadScope;
use aenv_node::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit};
use aenv_node::snapshot::repository::{RepositoryError, SnapshotCursor, SnapshotListFilter};
use aenv_node::snapshot::{
    CommandContext, CommittedSnapshot, ManagedLayer, SnapshotAlias, SnapshotId,
    SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use aenv_node::types::{ImageConfigs, SandboxResources};
use aenv_node::virtualization::VirtualizationMode;
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
            aenv_node::snapshot::repository::backends::central::STATUS_BUILDING,
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
            if build.status == aenv_node::snapshot::TemplateBuildStatus::Waiting
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
            assert_eq!(
                build.status,
                aenv_node::snapshot::TemplateBuildStatus::Error
            );
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
        aenv_node::snapshot::mock::write_mock_built_artifacts(workspace.path())
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
        let cursor = token.as_deref().map(|token| {
            snapshot_cursor_from_token(token).expect("the token this service minted parses")
        });
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
            aenv_node::snapshot::repository::backends::central::STATUS_BUILDING,
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

/// 🔴 The unbounded listing walks past its first page.
///
/// `list_scoped` is what every "count everything" caller reads. A listing that
/// stopped at one page would report a smaller catalog than there is, which does
/// not look like a bug from the outside — it looks like a cluster with fewer
/// templates.
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

/// 🔴 A failed build must release the template it was holding.
///
/// `builds_one_active_per_template` is what makes admission exclusive, and it
/// counts `pending`/`in_progress` rows — so a build that failed and *said so*
/// leaves a row that blocks the template for good unless the failure ends it
/// too. The reaper covers the builder that died without a word; nothing is
/// waiting on the one that reported.
#[tokio::test]
async fn a_failed_build_releases_the_template_it_held() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    catalog
        .create(template_record(id.clone(), None))
        .await
        .expect("creating should work");
    let failed = catalog
        .try_start_build(&id)
        .await
        .expect("the catalog should admit the build");
    catalog
        .mark_build_error(&id, TemplateBuildErrorReason::new("the build failed"))
        .await
        .expect("failing should work");

    let other = SnapshotId::generate();
    catalog
        .create(template_record(other.clone(), None))
        .await
        .expect("creating should work");
    let running = catalog
        .try_start_build(&other)
        .await
        .expect("a failed build must not hold the queue");

    assert_eq!(
        catalog
            .build_is_active(&failed.build_id)
            .await
            .expect("reading the build should work"),
        Some(false),
        "the failed build must be off the queue, or its template is blocked for good"
    );
    // The control: a build that is still running is still on it, so the
    // assertion above is about this build's failure rather than about the
    // question always answering `false`.
    assert_eq!(
        catalog
            .build_is_active(&running.build_id)
            .await
            .expect("reading the build should work"),
        Some(true)
    );
}

/// 🔴 A failed build can be retried, against the real primary key.
///
/// The build id was the template's — which is what the HTTP layer forces them
/// to look like — and the catalog keys a build row by it, so the second
/// admission collided with the first build row that ever existed. Measured on
/// this server before the fix: `a row with this id already exists`, arriving as
/// a 400 on the retry of every failed build. A template could be built exactly
/// once, ever.
///
/// The control is the second admission succeeding *and* naming a different
/// build: succeeding alone could be a catalog that quietly reused the row.
#[tokio::test]
async fn a_template_whose_build_failed_can_be_built_again() {
    let catalog = catalog!();
    let id = SnapshotId::generate();
    catalog
        .create(template_record(id.clone(), None))
        .await
        .expect("creating should work");
    let first = catalog
        .try_start_build(&id)
        .await
        .expect("the first build is admitted");
    catalog
        .mark_build_error(&id, TemplateBuildErrorReason::new("the build failed"))
        .await
        .expect("failing should work");

    let second = catalog
        .try_start_build(&id)
        .await
        .expect("a template whose build failed must be buildable again");

    assert_ne!(
        first.build_id, second.build_id,
        "the retry must be its own build, not the first one's row reused"
    );
    assert_eq!(
        catalog
            .build_is_active(&first.build_id)
            .await
            .expect("reading the build should work"),
        Some(false),
        "the failed build is off the queue"
    );
    assert_eq!(
        catalog
            .build_is_active(&second.build_id)
            .await
            .expect("reading the build should work"),
        Some(true),
        "and the retry is on it"
    );
}

/// 🔴 The template surface, against the server that hides the rows.
///
/// A template is created `waiting` and stays there until its first build
/// commits, and the resolvable reading is `status_group = 'ready'`. So on a
/// node reading PostgreSQL the whole template surface asked about rows the
/// query refuses to return: the get 404s, both listings omit it, the alias
/// does not resolve, and the build start that would have moved it out of
/// `waiting` 404s too — which is why it could never become resolvable either.
///
/// The unscoped halves below are the control. They are what every launch path
/// still uses, and they must go on refusing: a snapshot whose bytes are still
/// uploading must not start a VM.
#[tokio::test]
async fn a_template_that_has_never_been_built_is_readable_only_at_the_scoped_reading() {
    let concrete = catalog!();
    // 🔴 Through the trait, not the concrete client. `CentralSnapshotCatalog`
    // has inherent `*_scoped` methods with the same names, and inherent methods
    // win method resolution — so a test written against the concrete type
    // exercises those and says nothing about the `SnapshotCatalog` impl, which
    // is the one the repository calls and the one the whole
    // defect was in. Written this way the test fails when the override is
    // dropped; written the other way it does not.
    let catalog: &dyn SnapshotCatalog = concrete.as_ref();
    let id = SnapshotId::generate();
    let alias = unique_alias("pending-template");

    catalog
        .create(template_record(id.clone(), Some(&alias)))
        .await
        .expect("creating a template should work");

    // The control: the reading a launch reaches.
    assert!(
        catalog
            .get(&id.to_string())
            .await
            .expect("reading should work")
            .is_none(),
        "a template with no committed build is not something to launch"
    );
    assert!(
        catalog
            .resolve_alias(&alias)
            .await
            .expect("resolving should work")
            .is_none(),
        "nor is its name"
    );

    // And the reading every endpoint under /templates now uses.
    let seen = catalog
        .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
        .await
        .expect("reading should work")
        .expect("a template must be visible to the surface that manages it");
    assert_eq!(seen.id, id);
    assert_eq!(
        template_build_status(&seen),
        TemplateBuildStatus::Waiting,
        "and it must read as waiting, which is the state the surface reports"
    );

    assert_eq!(
        catalog
            .resolve_alias_scoped(&alias, CatalogReadScope::AnyStatus)
            .await
            .expect("resolving should work"),
        Some(id.clone()),
        "`aenv` turns every id-or-name argument into this lookup"
    );

    // 🔴 The listing, through the keyset page the endpoint actually calls
    // rather than through `list`: it is a different statement, with the ready
    // predicate spliced in at a different place, so agreeing about one says
    // nothing about the other.
    let page = catalog
        .list_page_scoped(SnapshotListFilter::templates(), CatalogReadScope::AnyStatus)
        .await
        .expect("listing should work");
    assert!(
        page.items.iter().any(|row| row.id == id),
        "a listing that cannot see `waiting` cannot see a newly created template"
    );
    let resolvable = catalog
        .list_page_scoped(
            SnapshotListFilter::templates(),
            CatalogReadScope::Resolvable,
        )
        .await
        .expect("listing should work");
    assert!(
        !resolvable.items.iter().any(|row| row.id == id),
        "the control: the resolvable listing still hides it"
    );
}

/// The build status of a template row, or a panic if it is not one.
fn template_build_status(record: &SnapshotRecord) -> TemplateBuildStatus {
    match &record.source {
        SnapshotSource::Template { build } => build.status,
        other => panic!("a template record, got {other:?}"),
    }
}
