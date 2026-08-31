//! Verifies the API half forwards cold-start image references unresolved.
//! A fake `regctl` proves this path performs no node-local image resolution.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::identity::NodeIdentity;
use crate::orchestrator::{
    DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator,
};
use crate::sandbox::mock::MockBackendFactory;
use aenv_core::api::impls::ApiImpl;

// Fully qualified reserved-domain reference keeps the fake resolver probe deterministic.
const IMAGE: &str = "registry.invalid/agentenv/cold-start:pinned";

struct Surface {
    api: Arc<ApiImpl>,
    regctl_dir: PathBuf,
}

async fn surface() -> Surface {
    let root = tempfile::tempdir().expect("a temp dir");

    // Symlink the fake executable to avoid concurrent `execve` `ETXTBSY` races.
    let deps_path = root.path().join("deps");
    let regctl = crate::cfg::regctl_path(&deps_path);
    let regctl_dir = regctl
        .parent()
        .expect("the regctl path has a parent")
        .to_path_buf();
    std::fs::create_dir_all(&regctl_dir).expect("create the fake regctl's directory");
    std::fs::write(regctl_dir.join("stdout"), "").expect("write the stdout fixture");
    std::fs::write(
        regctl_dir.join("stderr"),
        format!("failed to get manifest {IMAGE}: request failed: not found [http 404]: {{}}\n"),
    )
    .expect("write the stderr fixture");
    std::fs::write(regctl_dir.join("exit_code"), "1\n").expect("write the exit code fixture");
    std::os::unix::fs::symlink(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/regctl-recorder.sh"),
        &regctl,
    )
    .expect("link the fake regctl");

    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        FileBackedSandboxPersister::new_for_test(root.path().join("paused")),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an orchestrator");

    let snapshot_manager = Arc::new(crate::snapshot::mock::mock_snapshot_manager());

    let api = Arc::new(ApiImpl::new(
        orchestrator,
        Arc::clone(&snapshot_manager),
        None,
        crate::api::PausedSandboxWiring::new(
            Arc::new(DisabledPausedSandboxRegistry),
            Arc::clone(&snapshot_manager),
            &NodeIdentity::from_config(&Default::default()),
        ),
        Vec::new(),
        crate::api::ResumeWiring::api_half_for_test(),
    ));

    // Keep the temporary configuration and fake executable alive for the process.
    std::mem::forget(root);

    Surface { api, regctl_dir }
}

fn claims() -> aenv_core::api::impls::Claims {
    aenv_core::api::impls::Claims
}

fn host() -> Host {
    Host::from(http::uri::Authority::from_static("localhost"))
}

async fn cold_start(s: &Surface) -> SandboxesColdPostResponse {
    s.api
        .sandboxes_cold_post(
            &Method::POST,
            &host(),
            &CookieJar::new(),
            &claims(),
            &models::NewColdSandbox::new(IMAGE.to_string()),
        )
        .await
        .expect("the handler answers")
}

// Returns fake `regctl` argv, or `None` when resolution was never attempted.
fn regctl_argv(s: &Surface) -> Option<Vec<String>> {
    std::fs::read_to_string(s.regctl_dir.join("argv"))
        .ok()
        .map(|raw| raw.lines().map(ToString::to_string).collect())
}

#[tokio::test]
async fn a_cold_start_ships_the_reference_unresolved() {
    let s = surface().await;
    let response = cold_start(&s).await;
    assert_eq!(
        regctl_argv(&s),
        None,
        "🔴 this route must never resolve the image itself — that capability gap is what it \
         closes by dispatching instead of resolving — so regctl must not have run, got \
         {response:?}"
    );
    match &response {
        SandboxesColdPostResponse::Status500_ServerError(error) => {
            assert!(
                !error.message.contains("no sandbox runtime"),
                "this must not be the old door refusal — the whole point of the fix is that \
                 this route no longer refuses outright — got {:?}",
                error.message
            );
            assert!(
                error.message.contains("resolves OCI images itself"),
                "the fixture's orchestrator has no RemoteSandboxBackendFactory to dial a \
                 node with, so the request should run out of road at \
                 SandboxBackendFactory::build_from_image_ref's default refusal — reaching \
                 that refusal (rather than the door refusal, and rather than a panic) is the \
                 evidence that sandboxes_cold_post built an unresolved-image launch source \
                 and handed it to the orchestrator, got {:?}",
                error.message
            );
        }
        other => panic!(
            "expected the fixture's local factory to refuse an unresolved-image build (it \
             inherits SandboxBackendFactory::build_from_image_ref's default rather than \
             overriding it, unlike RemoteSandboxBackendFactory), got {other:?}"
        ),
    }
}
