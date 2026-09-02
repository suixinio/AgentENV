//! `SnapshotManager` against POSIX byte storage and a test-owned catalog.
//! The combined fixture exercises stage, commit, resolve, and delete across
//! the node/API storage split.

use std::sync::Arc;

use aenv_core::snapshot::SnapshotManager;

use crate::snapshot::mock::{write_mock_built_artifacts, InMemorySnapshotCatalog};
use crate::snapshot::repository::backends::storage::{PosixFsBackend, PosixFsBackendConfig};
use crate::snapshot::repository::StagedSnapshot;
use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
use aenv_core::snapshot::{
    CallerOwnedArtifacts, CapturedSandboxSnapshot, RepositoryError, SnapshotPublishSource,
    SnapshotRecord,
};
use std::path::Path;
use tempfile::TempDir;

fn test_manager(root: &Path) -> SnapshotManager {
    let backend = PosixFsBackend::new(PosixFsBackendConfig {
        root: root.join("repository"),
        cache_root: Some(root.join("runtime-cache")),
        runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
    })
    .expect("posix backend");
    let (repository, runtime_resolver) = backend.into_parts();
    SnapshotManager::from_parts(
        InMemorySnapshotCatalog::in_front_of(&repository),
        Some(runtime_resolver),
        None,
    )
}

#[tokio::test]
async fn a_manager_with_no_runtime_resolver_refuses_rather_than_panicking() {
    let record = SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());

    let tempdir = TempDir::new().expect("tempdir should exist");
    let repository = {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, _) = backend.into_parts();
        repository
    };

    let without = SnapshotManager::from_parts(Arc::clone(&repository), None, None);
    let err = without
        .resolve_runnable(record.clone())
        .await
        .expect_err("a manager with no runtime resolver must refuse to resolve");
    let typed = err
        .downcast_ref::<crate::snapshot::RepositoryError>()
        .unwrap_or_else(|| panic!("the refusal must be a typed RepositoryError: {err:?}"));
    assert!(
        matches!(typed, crate::snapshot::RepositoryError::Unsupported { .. }),
        "the refusal must say the operation is unsupported here, not that something \
         went wrong reaching it: {typed:?}"
    );

    // The control: the same record, through a manager that *does* hold a
    // resolver, gets past the gate and fails on the artifacts instead.
    let with = test_manager(tempdir.path());
    let err = with
        .resolve_runnable(record)
        .await
        .expect_err("nothing was ever published, so resolving must fail");
    let typed = err
        .downcast_ref::<crate::snapshot::RepositoryError>()
        .unwrap_or_else(|| panic!("the resolver's own failure is typed too: {err:?}"));
    assert!(
        !matches!(typed, crate::snapshot::RepositoryError::Unsupported { .. }),
        "a manager that holds a resolver reported the no-resolver refusal, so the \
         assertion above proves nothing: {typed:?}"
    );
}

async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
    let workspace = TempDir::new().expect("tempdir should exist");
    let (_, _, manifest) =
        write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
    let metadata = SnapshotPublishMetadata {
        id: snapshot_id,
        alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
        ..SnapshotPublishMetadata::mock()
    };
    manager
        .publish(metadata, manifest)
        .await
        .expect("seed publish should work");
}

#[tokio::test]
async fn repository_management_methods_delegate_to_committed_store() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    seed_built_snapshot(&manager, snapshot_id.clone(), "managed").await;

    let resolved = manager
        .resolve_committed_alias("managed")
        .await
        .expect("resolve alias should work");
    assert_eq!(resolved, Some(snapshot_id.clone()));

    let loaded = manager
        .get("managed")
        .await
        .expect("load should work")
        .expect("snapshot should exist");
    assert_eq!(loaded.id, snapshot_id);

    let listed = manager
        .list_page(crate::snapshot::repository::SnapshotListFilter::matches_all())
        .await
        .expect("list should work");
    assert_eq!(listed.items.len(), 1);
    assert!(listed.next.is_none());

    manager.delete("managed").await.expect("delete should work");
    assert!(manager
        .get("managed")
        .await
        .expect("load after delete should work")
        .is_none());
}

#[tokio::test]
async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

    let runnable = manager
        .load_runnable("runnable")
        .await
        .expect("load runnable should work")
        .expect("runnable snapshot should exist");

    assert_eq!(runnable.record().id, snapshot_id);
    assert!(runnable.manifest().rootfs.image_config_path.exists());
    assert!(runnable.manifest().vm_state.path.exists());
}

#[tokio::test]
async fn a_staged_snapshot_has_bytes_on_disk_and_no_row_anywhere() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let workspace = TempDir::new().expect("tempdir should exist");
    let (_, _, manifest) =
        write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
    let metadata = SnapshotPublishMetadata {
        id: snapshot_id.clone(),
        alias: Some(SnapshotAlias::parse("staged-only").expect("alias should parse")),
        ..SnapshotPublishMetadata::mock()
    };

    let handle = manager
        .stage(metadata, manifest)
        .await
        .expect("staging should work");

    let artifacts = tempdir
        .path()
        .join("repository")
        .join("snapshots")
        .join(snapshot_id.to_string());
    assert!(
        artifacts.join("vm_state.bin").exists(),
        "the bytes must be durable before the row exists"
    );
    assert!(
        manager
            .get(snapshot_id.to_string())
            .await
            .expect("get should work")
            .is_none(),
        "a staged snapshot must not be resolvable by id"
    );
    assert!(
        manager
            .resolve_committed_alias("staged-only")
            .await
            .expect("resolve should work")
            .is_none(),
        "a staged snapshot must not be resolvable by alias"
    );
    assert!(manager
        .list_page(crate::snapshot::repository::SnapshotListFilter::matches_all())
        .await
        .expect("list should work")
        .items
        .is_empty());

    // The control: one more call flips all three answers.
    let (staged, local) = handle.into_parts();
    let record = manager
        .commit_staged(staged)
        .await
        .expect("commit should work");
    manager.advertise_committed(&record, local).await;

    assert!(manager
        .get(snapshot_id.to_string())
        .await
        .expect("get should work")
        .is_some());
    assert_eq!(
        manager
            .resolve_committed_alias("staged-only")
            .await
            .expect("resolve should work"),
        Some(snapshot_id)
    );
    assert_eq!(
        manager
            .list_page(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work")
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn a_manager_staged_snapshot_commits_after_a_serde_round_trip() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let workspace = TempDir::new().expect("tempdir should exist");
    let (_, _, manifest) =
        write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
    let metadata = SnapshotPublishMetadata {
        id: snapshot_id.clone(),
        alias: Some(SnapshotAlias::parse("round-tripped").expect("alias should parse")),
        ..SnapshotPublishMetadata::mock()
    };

    let handle = manager
        .stage(metadata, manifest)
        .await
        .expect("staging should work");
    let encoded = serde_json::to_vec(handle.staged()).expect("staged value should serialize");
    drop(handle);

    let decoded: StagedSnapshot =
        serde_json::from_slice(&encoded).expect("staged value should deserialize");
    let record = manager
        .commit_staged(decoded)
        .await
        .expect("a round-tripped staged snapshot must commit");

    assert_eq!(record.id, snapshot_id);
    assert!(manager
        .get("round-tripped")
        .await
        .expect("get should work")
        .is_some());
}

/// Builds a row staged on another node.
pub fn staged_elsewhere(id: SnapshotId, source_sandbox_id: &str) -> StagedSnapshot {
    StagedSnapshot {
        commit: crate::snapshot::repository::SnapshotCommit {
            id,
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.to_string(),
            },
            resources: Default::default(),
            created_at_unix_ms: Some(1_700_000_000_000),
            origin_node_id: Some("the-node-holding-the-bytes".to_string()),
            committed: crate::snapshot::CommittedSnapshot::mock(),
        },
        staged_at_unix_ms: 1_700_000_000_000,
        origin_node_id: "the-node-holding-the-bytes".to_string(),
    }
}

pub fn capture_of(source_sandbox_id: &str, id: SnapshotId) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id,
        source: SnapshotPublishSource::Sandbox {
            source_sandbox_id: source_sandbox_id.to_string(),
        },
        ..SnapshotPublishMetadata::mock()
    }
}

#[tokio::test]
async fn a_capture_staged_elsewhere_is_committed_rather_than_staged_again() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());
    let snapshots = tempdir.path().join("repository").join("snapshots");
    let sandbox = "the-sandbox-both-halves-are-talking-about";

    // The half that stages here: real artifacts, and the bytes land under
    // the id this process chose.
    let workspace = TempDir::new().expect("tempdir should exist");
    let (_, _, manifest) =
        write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
    let local_id = SnapshotId::generate();
    let local = manager
        .publish_captured(
            capture_of(sandbox, local_id.clone()),
            CapturedSandboxSnapshot::local(CallerOwnedArtifacts::new(manifest)),
        )
        .await
        .expect("a local capture should publish");
    assert_eq!(local.id, local_id, "a local capture must keep its own id");

    // The other half: a row somebody else staged, offered under an id this
    // process minted and an alias only this process knows about.
    let staged_id = SnapshotId::generate();
    let proposed_id = SnapshotId::generate();
    let mut proposal = capture_of(sandbox, proposed_id.clone());
    proposal.alias = Some(SnapshotAlias::parse("the-name-the-user-asked-for").expect("alias"));
    let adopted = manager
        .publish_captured(
            proposal,
            CapturedSandboxSnapshot::staged(staged_elsewhere(staged_id.clone(), sandbox)),
        )
        .await
        .expect("an adopted staging should commit");

    assert_eq!(
        adopted.id, staged_id,
        "the row must carry the id the bytes were written under"
    );
    assert_ne!(
        adopted.id, proposed_id,
        "this half's proposed id must not win: no bytes were ever written under it"
    );
    assert_eq!(
        adopted.alias.as_ref().map(ToString::to_string).as_deref(),
        Some("the-name-the-user-asked-for"),
        "the alias is the committer's and staging never had it"
    );
    assert_eq!(
        adopted.created_at_unix_ms, 1_700_000_000_000,
        "the row must keep the staging node's created_at_unix_ms, not the committing node's own clock"
    );

    assert!(
        !snapshots
            .join(staged_id.to_string())
            .join("vm_state.bin")
            .exists(),
        "adopting a staging must not write bytes this process does not have"
    );
    assert!(
        snapshots
            .join(local_id.to_string())
            .join("vm_state.bin")
            .exists(),
        "the locally staged capture's bytes are missing, so the assertion above proves nothing"
    );

    // Both rows are announced, which is what makes the commit a commit.
    assert!(manager
        .get(staged_id.to_string())
        .await
        .expect("get should work")
        .is_some());
    assert!(manager
        .get(local_id.to_string())
        .await
        .expect("get should work")
        .is_some());
    assert!(manager
        .resolve_committed_alias("the-name-the-user-asked-for")
        .await
        .expect("resolve should work")
        .is_some());
}

#[tokio::test]
async fn a_staging_of_a_different_sandbox_is_refused_and_nothing_is_announced() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let manager = test_manager(tempdir.path());

    let mismatched_id = SnapshotId::generate();
    let err = manager
        .publish_captured(
            capture_of("the-sandbox-this-half-asked-about", SnapshotId::generate()),
            CapturedSandboxSnapshot::staged(staged_elsewhere(
                mismatched_id.clone(),
                "some-other-sandbox-entirely",
            )),
        )
        .await
        .expect_err("a staging of another sandbox must be refused");
    assert!(
        matches!(err, RepositoryError::InvalidRequest { .. }),
        "expected an invalid-request refusal, got {err:?}"
    );
    assert!(
        manager
            .get(mismatched_id.to_string())
            .await
            .expect("get should work")
            .is_none(),
        "the refused staging was announced anyway"
    );

    // The control face: the same call, sandbox ids agreeing, commits.
    let matching_id = SnapshotId::generate();
    let record = manager
        .publish_captured(
            capture_of("the-sandbox-this-half-asked-about", SnapshotId::generate()),
            CapturedSandboxSnapshot::staged(staged_elsewhere(
                matching_id.clone(),
                "the-sandbox-this-half-asked-about",
            )),
        )
        .await
        .expect("a staging of the sandbox that was asked about should commit");
    assert_eq!(record.id, matching_id);
    assert!(manager
        .get(matching_id.to_string())
        .await
        .expect("get should work")
        .is_some());
}
