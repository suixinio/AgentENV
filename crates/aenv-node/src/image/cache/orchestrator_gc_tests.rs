//! Verifies running sandboxes keep runtime image commits pinned in the real
//! node-local layer cache.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;
use tempfile::TempDir;

use super::test_support::{test_local_image_services_from_service, ImageCacheService};
use crate::cfg::ResolvedImageCacheConfig;
use crate::image::RecordingRuntimeImageRefs;
use crate::orchestrator::{
    CreateSandboxRequest, InMemoryMetadataStore, Orchestrator, SandboxExpiry, SandboxLaunchSource,
    SandboxTimeoutAction,
};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::mock::{MockBackendFactory, MockBehavior};
use crate::sandbox::{RuntimeArtifactSet, SandboxNetworkPolicy, SandboxRuntimeInfo};

fn setup() {
    crate::logging::init_for_tests();
}

fn create_request(
    timeout_secs: Option<u64>,
    _user_metadata: &[(&str, &str)],
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        expiry: match timeout_secs {
            Some(secs) => SandboxExpiry::After(Duration::from_secs(secs)),
            None => SandboxExpiry::AfterConfiguredDefault,
        },
        timeout_action: SandboxTimeoutAction::Pause,
        user_metadata: None,
        env_vars: None,
        network_policy: SandboxNetworkPolicy::default(),
        custom_extension_params: None,
        control_plane_config: None,
        execution_id: None,
        auto_resume: false,
        secure: false,
        preferred_node_id: None,
    }
}

fn write_local_commit_image_config(path: &Path, file: &Path, digest: &str, size: u64) {
    std::fs::create_dir_all(path.parent().expect("image config parent"))
        .expect("create image config dir");
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&json!({
            "repoBlobUrl": "",
            "lowers": [{
                "file": file.display().to_string(),
                "digest": digest,
                "size": size
            }],
            "upper": {},
            "resultFile": ""
        }))
        .expect("serialize image config"),
    )
    .expect("write image config");
}

#[tokio::test]
async fn a_running_sandbox_keeps_its_commit_pinned_after_its_source_config_was_evicted(
) -> Result<()> {
    setup();
    let temp = TempDir::new().expect("tempdir");
    let root_dir = temp.path().join("image-cache");
    let image_cache = Arc::new(ImageCacheService::from_resolved_config(
        ResolvedImageCacheConfig {
            commit_store: root_dir.join("commits"),
            remote_blocks_dir: root_dir.join("remote-blocks"),
            root_dir: root_dir.clone(),
            remote_blocks_size_gb: 10,
            capacity_bytes: None,
        },
    ));

    let source = temp.path().join("source.commit");
    std::fs::write(&source, b"pinned").expect("write source commit");
    let commit_file = image_cache
        .import_hard_commit_trusted_descriptor(&source, "sha256:pinned", 6)
        .await
        .expect("import the commit");

    let source_config = root_dir.join("configs/source-image.json");
    let runtime_config = temp.path().join("runtime/image.json");
    write_local_commit_image_config(&source_config, &commit_file, "sha256:pinned", 6);
    write_local_commit_image_config(&runtime_config, &commit_file, "sha256:pinned", 6);

    let behavior = Arc::new(MockBehavior::new());
    behavior.set_source_config_paths(vec![source_config.clone()]);
    behavior.set_runtime_info(SandboxRuntimeInfo {
        runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(vec![runtime_config]),
        ..Default::default()
    });
    let mut orchestrator = Arc::new(Orchestrator::from_test_parts(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(behavior),
        Duration::from_secs(600),
        Arc::new(RecordingRuntimeImageRefs::default()),
        "orchestrator-image-cache-test-seed",
    ));
    Arc::get_mut(&mut orchestrator)
        .expect("orchestrator should be uniquely owned")
        .image_refs = test_local_image_services_from_service(
        Arc::clone(&image_cache),
        None,
        Duration::from_secs(0),
    )
    .runtime_refs;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    std::fs::remove_file(&source_config).expect("evict source config");

    // Maintenance refuses to reclaim anything until the startup reconcile ran.
    image_cache.startup_reconcile().await?;
    let running: Vec<(String, Vec<PathBuf>)> = orchestrator
        .collect_running_artifacts()
        .await
        .into_iter()
        .map(|(id, artifacts)| {
            (
                id.to_string(),
                artifacts.into_overlaybd_image_config_paths(),
            )
        })
        .collect();
    assert!(
        running.iter().any(|(id, _)| id == &created.id.to_string()),
        "the running set does not name the sandbox, so this test proves nothing"
    );
    let summary = image_cache
        .run_maintenance(running, None, Duration::from_secs(0))
        .await
        .expect("run gc");

    assert_eq!(summary.collected, 0);
    assert!(commit_file.exists());
    assert!(
        summary.retained >= 1,
        "the running sandbox's commit must be retained through its runtime config"
    );
    Ok(())
}
