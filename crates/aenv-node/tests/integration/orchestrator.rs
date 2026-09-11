use crate::common;

use aenv_node::cfg::ConfigManager;
use aenv_node::orchestrator::{
    CommittingPausePublisher, CreateSandboxRequest, ForkChildren, InMemoryMetadataStore,
    NewTimeout, Orchestrator, ProxyLookupResult, PublishedPause, SandboxExpiry,
    SandboxLaunchSource, SandboxState, SandboxTimeoutAction,
};
use aenv_node::sandbox::{FirecrackerSandboxFactory, SandboxNetworkPolicy};
use aenv_node::snapshot::{
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotSource,
    StartupCommand,
};

use anyhow::Result;
use envd::process::{ListRequest, ProcessClient};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{timeout, Duration};
use tonic::Request;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

async fn envd_process_list_status(
    target: &aenv_node::orchestrator::ProxyTarget,
    access_token: Option<&str>,
) -> Result<envd::reqwest::StatusCode> {
    let port = ConfigManager::global_config().tools.control_plane_port;
    let mut request = envd::reqwest::Client::new()
        .post(format!("http://{}:{port}/process.Process/List", target.ip));
    if let Some(access_token) = access_token {
        request = request.header("X-Access-Token", access_token);
    }
    Ok(request.send().await?.status())
}

async fn assert_envd_process_list_succeeds(
    target: &aenv_node::orchestrator::ProxyTarget,
    access_token: &str,
) -> Result<()> {
    let port = ConfigManager::global_config().tools.control_plane_port;
    let mut client =
        ProcessClient::connect(&format!("http://{}:{port}", target.ip), Some(access_token)).await?;
    client.list(Request::new(ListRequest {})).await?;
    Ok(())
}

#[tokio::test]
async fn orchestrator_lifecycle() -> Result<()> {
    common::setup().await;
    timeout(TEST_TIMEOUT, async {
        let root = tempdir()?;
        let (builder, snapshot_manager, _) = common::snapshot_test_parts(root.path());
        let alias = format!("orchestrator-test-shared-{}", Uuid::now_v7());

        let stored = builder
            .build_and_publish(
                &snapshot_manager,
                common::default_rootfs_template_build_spec()
                    .alias(alias)
                    .run("mkdir -p /workspace"),
            )
            .await?;
        let runnable = snapshot_manager.resolve_runnable(stored).await?;
        let snapshot_manager = Arc::new(snapshot_manager);

        let store = InMemoryMetadataStore::new();
        let factory = FirecrackerSandboxFactory::new();
        let orchestrator = Orchestrator::new(
            aenv_node::sandbox::AccessTokenSeedPolicy::MayGenerate,
            store,
            factory,
            aenv_node::image::DisabledRuntimeImageRefs::shared(),
            Arc::new(CommittingPausePublisher::new(Arc::clone(&snapshot_manager))),
            aenv_node::orchestrator::NoGrants::shared(),
        )
        .await?;
        let case_id = Uuid::now_v7().to_string();

        let request = CreateSandboxRequest {
            traffic_access_token: None,
            source: SandboxLaunchSource::Snapshot(Box::new(runnable)),
            expiry: SandboxExpiry::After(Duration::from_secs(30)),
            timeout_action: SandboxTimeoutAction::Pause,
            user_metadata: Some(
                [
                    ("team".to_string(), "alpha".to_string()),
                    ("case_id".to_string(), case_id.clone()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
            env_vars: None,
            network_policy: SandboxNetworkPolicy::default(),
            auto_resume: false,
            custom_extension_params: None,
            control_plane_config: None,
            execution_id: None,
            secure: true,
            preferred_node_id: None,
        };

        let created = orchestrator.create_sandbox(request).await?;
        assert_eq!(created.state, SandboxState::Running);
        let access_token = orchestrator
            .get_envd_access_token(&created)
            .expect("secure sandbox access token");

        let sandbox_id = created.id;
        let lookup = orchestrator.proxy_lookup_for(&sandbox_id).await?;
        assert!(
            matches!(lookup, ProxyLookupResult::Ready(_)),
            "expected proxy lookup Ready for sandbox {sandbox_id}, got {lookup:?}"
        );
        let ProxyLookupResult::Ready(target) = lookup else {
            unreachable!();
        };
        assert_eq!(
            envd_process_list_status(&target, None).await?,
            envd::reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            envd_process_list_status(&target, Some("wrong-token")).await?,
            envd::reqwest::StatusCode::UNAUTHORIZED
        );
        assert_envd_process_list_succeeds(&target, access_token.expose()).await?;

        let child = orchestrator
            .fork_sandbox(sandbox_id, ForkChildren::Fresh(1), NewTimeout::UseExisting)
            .await?
            .pop()
            .expect("one fork result")?;
        let child_token = orchestrator
            .get_envd_access_token(&child)
            .expect("secure fork access token");
        assert_ne!(child_token, access_token);
        let ProxyLookupResult::Ready(child_target) =
            orchestrator.proxy_lookup_for(&child.id).await?
        else {
            panic!("secure fork should have a proxy route");
        };
        assert_eq!(
            envd_process_list_status(&child_target, Some(access_token.expose())).await?,
            envd::reqwest::StatusCode::UNAUTHORIZED
        );
        assert_envd_process_list_succeeds(&child_target, child_token.expose()).await?;
        orchestrator.delete_sandbox(child.id).await?;

        let fetched = orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("sandbox metadata should exist after create");
        assert_eq!(fetched.id, sandbox_id);
        assert_eq!(fetched.state, SandboxState::Running);

        let paused = orchestrator.pause_sandbox(sandbox_id).await?;
        let PublishedPause::Committed(paused_snapshot_id) = paused.published else {
            panic!("a pause this process commits answers with the row it wrote: {paused:?}");
        };
        assert!(
            orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
            "a paused sandbox exists only as its snapshot"
        );
        assert_eq!(
            orchestrator.proxy_lookup_for(&sandbox_id).await?,
            ProxyLookupResult::NotFound
        );

        let paused_record = snapshot_manager
            .get(paused_snapshot_id.to_string())
            .await?
            .expect("the pause committed a catalog row");
        let paused_config = paused_record
            .committed
            .as_ref()
            .and_then(|committed| committed.paused_sandbox.as_ref())
            .expect("a pause row says how to bring the sandbox back");
        assert!(paused_config.secure);
        assert_eq!(paused_config.template_id, created.snapshot_id);

        let paused_runnable = snapshot_manager.resolve_runnable(paused_record).await?;
        let resumed = orchestrator
            .restore_sandbox(
                sandbox_id,
                CreateSandboxRequest {
                    traffic_access_token: None,
                    source: SandboxLaunchSource::Snapshot(Box::new(paused_runnable)),
                    expiry: SandboxExpiry::After(Duration::from_secs(120)),
                    timeout_action: SandboxTimeoutAction::Pause,
                    user_metadata: None,
                    env_vars: None,
                    network_policy: SandboxNetworkPolicy::default(),
                    auto_resume: false,
                    custom_extension_params: None,
                    control_plane_config: None,
                    execution_id: None,
                    secure: true,
                    preferred_node_id: None,
                },
            )
            .await?;
        assert_eq!(resumed.id, sandbox_id);
        assert_ne!(resumed.execution_id, created.execution_id);
        assert_eq!(resumed.state, SandboxState::Running);
        assert_eq!(resumed.timeout, Some(Duration::from_secs(120)));
        let restarted = orchestrator;
        let lookup = restarted.proxy_lookup_for(&sandbox_id).await?;
        assert!(
            matches!(lookup, ProxyLookupResult::Ready(_)),
            "expected proxy lookup Ready after resume for sandbox {sandbox_id}, got {lookup:?}"
        );
        let resumed_token = restarted
            .get_envd_access_token(&resumed)
            .expect("resumed secure sandbox access token");
        assert_eq!(resumed_token, access_token);
        let ProxyLookupResult::Ready(target) = lookup else {
            unreachable!();
        };
        assert_eq!(
            envd_process_list_status(&target, None).await?,
            envd::reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            envd_process_list_status(&target, Some("wrong-token")).await?,
            envd::reqwest::StatusCode::UNAUTHORIZED
        );
        assert_envd_process_list_succeeds(&target, resumed_token.expose()).await?;

        restarted.delete_sandbox(sandbox_id).await?;
        let deleted = restarted.get_sandbox(&sandbox_id).await?;
        assert!(deleted.is_none(), "sandbox should be removed after delete");
        let lookup = restarted.proxy_lookup_for(&sandbox_id).await?;
        assert_eq!(lookup, ProxyLookupResult::NotFound);

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn orchestrator_capture_snapshot_can_be_published_and_relaunched() -> Result<()> {
    common::setup().await;
    timeout(TEST_TIMEOUT, async {
        let root = tempdir()?;
        let (builder, snapshot_manager, _) = common::snapshot_test_parts(root.path());
        let base_alias = format!("orchestrator-capture-base-{}", Uuid::now_v7());

        let stored = builder
            .build_and_publish(
                &snapshot_manager,
                common::default_rootfs_template_build_spec()
                    .alias(base_alias)
                    .run("mkdir -p /workspace && echo captured-base > /workspace/base.txt")
                    .env("CAPTURE_CONTEXT", "preserved")
                    .workdir("/workspace")
                    .start_cmd("sleep 1000000")
                    .ready_cmd("test -f base.txt"),
            )
            .await?;
        let runnable = snapshot_manager.resolve_runnable(stored).await?;

        let orchestrator =
            Orchestrator::with_in_memory_store(FirecrackerSandboxFactory::new()).await;
        let created = orchestrator
            .create_sandbox(CreateSandboxRequest {
                traffic_access_token: None,
                source: SandboxLaunchSource::Snapshot(Box::new(runnable)),
                expiry: SandboxExpiry::After(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: None,
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
                control_plane_config: None,
                execution_id: None,
                secure: false,
                preferred_node_id: None,
            })
            .await?;
        let sandbox_id = created.id;
        let capture = orchestrator.capture_snapshot(sandbox_id).await?;
        assert_eq!(capture.metadata.id, sandbox_id);
        assert_eq!(capture.metadata.state, SandboxState::Running);
        assert_eq!(capture.metadata.context.workdir, "/workspace");
        assert_eq!(
            capture
                .metadata
                .context
                .env_vars
                .get("CAPTURE_CONTEXT")
                .map(String::as_str),
            Some("preserved")
        );
        assert!(matches!(
            capture.metadata.startup.as_ref(),
            Some(StartupCommand {
                start_cmd,
                ready_cmd,
                context,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let published_alias = format!("orchestrator-captured-{}", Uuid::now_v7());
        let sandbox_id_str = sandbox_id.to_string();
        let published = snapshot_manager
            .publish_captured(
                SnapshotPublishMetadata {
                    id: SnapshotId::generate(),
                    alias: Some(SnapshotAlias::parse(&published_alias)?),
                    source: SnapshotPublishSource::Sandbox {
                        source_sandbox_id: sandbox_id_str.clone(),
                    },
                    context: capture.metadata.context.clone(),
                    startup: capture.metadata.startup.clone(),
                    resources: capture.metadata.resources,
                    runtime_versions: capture.metadata.runtime_versions.clone(),
                    virtualization_mode: capture.metadata.virtualization_mode,
                    huge_pages: false,
                    image_configs: capture.metadata.image_configs.clone(),
                    custom_extension_params: None,
                    paused_sandbox: None,
                },
                capture.captured_snapshot,
            )
            .await?;
        let record = snapshot_manager
            .get(published.id.to_string())
            .await?
            .expect("published snapshot record should exist");
        assert!(matches!(
            &record.source,
            SnapshotSource::Sandbox {
                source_sandbox_id
            } if source_sandbox_id == &sandbox_id_str
        ));
        let committed = record
            .committed
            .as_ref()
            .expect("published snapshot should be committed");
        assert_eq!(committed.context.workdir, "/workspace");
        assert_eq!(
            committed
                .context
                .env_vars
                .get("CAPTURE_CONTEXT")
                .map(String::as_str),
            Some("preserved")
        );
        assert!(matches!(
            committed.startup.as_ref(),
            Some(StartupCommand {
                start_cmd,
                ready_cmd,
                context,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let captured_runnable = snapshot_manager.resolve_runnable(published).await?;
        let relaunched = orchestrator
            .create_sandbox(CreateSandboxRequest {
                traffic_access_token: None,
                source: SandboxLaunchSource::Snapshot(Box::new(captured_runnable)),
                expiry: SandboxExpiry::After(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: None,
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
                control_plane_config: None,
                execution_id: None,
                secure: false,
                preferred_node_id: None,
            })
            .await?;
        assert_eq!(relaunched.state, SandboxState::Running);
        assert!(
            matches!(
                orchestrator.proxy_lookup_for(&relaunched.id).await?,
                ProxyLookupResult::Ready(_)
            ),
            "relaunched sandbox should be proxyable through orchestrator"
        );

        orchestrator.delete_sandbox(relaunched.id).await?;
        orchestrator.delete_sandbox(sandbox_id).await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}
