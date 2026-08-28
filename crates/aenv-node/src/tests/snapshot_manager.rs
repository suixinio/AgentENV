//! `SnapshotManager` against a real POSIX repository.
//!
//! 🔴 In `aenv-node`: every one of these builds a `PosixFsBackend`, whose
//! importing half reads overlaybd layers and so lives in this crate. The
//! manager itself is `aenv-core`'s and is driven through its public API.
//!
//! 🔴 And every one of them supplies its own catalog. A node's repository
//! carries `NoSnapshotCatalog` and refuses every catalog call, because the
//! catalog is PostgreSQL and lives in `aenv-api`. What these tests exercise —
//! stage, commit, resolve, delete — spans both halves, so they put
//! `InMemorySnapshotCatalog` in front of the POSIX byte half and stand in for
//! the committer. That is a fair stand-in for the flow and *not* a claim that a
//! node can commit: `a_staged_snapshot_has_bytes_on_disk_and_no_row_anywhere`
//! is the test that says what staging alone does.

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

/// 🔴 A manager assembled without a runtime resolver — which is what
/// `aenv-api` gets — must *refuse* a resolve, not abort the process.
///
/// The refusal is typed: `RepositoryError::Unsupported`, downcastable, so
/// a caller that forgot to fork on `ApiImpl::runs_sandbox_runtime` gets
/// a legible 5xx on one request instead of taking every in-flight request
/// down with it.
///
/// The control is the second half: the *same* record through a manager
/// that does hold a resolver reaches the resolver and fails on the missing
/// artifacts instead. Without it, "returns an error" would be satisfied by
/// a manager that can never resolve anything at all.
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
        .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
        .await
        .expect("list should work");
    assert_eq!(listed.len(), 1);

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

/// 🔴 P12, second half, against a real backend rather than a fake catalog.
/// After `stage` and before `commit_staged` the artifacts are on disk and
/// neither read path can see the snapshot. The control is the commit: the
/// same two reads, run again after it, must both find it.
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
        .stage(metadata, manifest, None)
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
        .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
        .await
        .expect("list should work")
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
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work")
            .len(),
        1
    );
}

/// The staged value that reached the commit has to be the one that could
/// have travelled — so run the round trip through the manager's own API,
/// not just the repository's.
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
        .stage(metadata, manifest, None)
        .await
        .expect("staging should work");
    let encoded = serde_json::to_vec(handle.staged()).expect("staged value should serialize");
    // 🔴 Dropped before the commit: the local half is gone, and the commit
    // still has to work. That is the property `aenv-api` depends on.
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

/// Builds a row staged somewhere this process cannot read.
pub fn staged_elsewhere(id: SnapshotId, source_sandbox_id: &str) -> StagedSnapshot {
    StagedSnapshot {
        commit: crate::snapshot::repository::SnapshotCommit {
            id,
            // 🔴 Unnamed, always. A staging node is never told the alias —
            // staging does not read one — so a test that seeded one here
            // would be proving the committer *kept* a name rather than
            // that it supplied one.
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.to_string(),
            },
            resources: Default::default(),
            created_at_unix_ms: Some(1_700_000_000_000),
            committed: crate::snapshot::CommittedSnapshot::mock(),
        },
        staged_at_unix_ms: 1_700_000_000_000,
        origin_node_id: "the-node-holding-the-bytes".to_string(),
        execution_id: None,
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

/// A capture that arrived already staged is committed where it lies, and
/// the row it produces is the staging node's identity wearing this half's
/// name.
///
/// 🔴 Both halves run in this one test, against the same repository, and
/// each is the other's control. The local publish is what makes "no
/// directory was written for the adopted id" mean something: the same
/// assertion against the same directory finds the locally staged snapshot's
/// bytes sitting there. Without it, a `stage_captured` that had silently
/// stopped writing anything at all would pass.
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

    // 🔴 The pair that carries the whole claim: no *artifact* was written
    // here for the adopted snapshot, and the identical look at the locally
    // staged one finds its bytes. The directory itself is not the
    // assertion — committing a row creates one either way, to hold the
    // record — so `vm_state.bin` is what separates "the row was announced"
    // from "the bytes were written here".
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

/// A staging that names a different sandbox is refused, and refused before
/// anything is announced.
///
/// 🔴 The control is the same call with the sandbox ids agreeing. Without
/// it "the row was not committed" is satisfied by an `adopt_staged` that
/// refuses everything, which is the exact failure that would make the
/// published arm silently useless.
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
