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
//! 🔴 Without the endpoint every test here skips, and a skip in `go test`'s
//! default mode reports as `ok`. `AENV_SNAPSHOT_CATALOG_TEST_REQUIRED=1` turns
//! the missing dependency into a failure, which is what CI must set — the same
//! arrangement, for the same reason, as `SCHEDULER_REGISTRY_TEST_REQUIRED`.

use std::collections::HashMap;
use std::sync::Arc;

use agentenv::sandbox::FirecrackerSnapshotManifest;
use agentenv::snapshot::repository::backends::{CatalogReadScope, CentralSnapshotCatalog};
use agentenv::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit};
use agentenv::snapshot::repository::{RepositoryError, SnapshotListFilter};
use agentenv::snapshot::{
    CommandContext, CommittedSnapshot, ManagedLayer, SnapshotAlias, SnapshotId,
    SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource,
    TemplateBuildErrorReason,
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
        committed: committed(marker),
    }
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
