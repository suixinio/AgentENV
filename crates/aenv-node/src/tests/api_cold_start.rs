//! What `POST /sandboxes-cold` does with the image reference it is given.
//!
//! A cold start's first act used to be resolving an OCI image *in the process
//! serving the call*: `regctl` fetches the manifest and converts the layers
//! into a local overlaybd image for a local Firecracker VM. The `api` Pod
//! installs no `regctl` — that is a node's tooling — so the call died on step
//! one with `regctl is required for OCI registry access`, and the refusal that
//! actually names the problem (`RemoteSandboxBackendFactory::build`) sat behind
//! that resolve and was never reached. The operator was told a tool was
//! missing; the truth was that this half cannot cold-start at all.
//!
//! 🔴 In `aenv-node` even though the surface under test is `aenv-core`'s
//! `api::impls::sandbox`. `ApiImpl::new` no longer takes an image resolver at
//! all — the arm that resolved in-process is deleted — but the fixture still
//! installs a fake `regctl` and points the process config at it, so "nothing
//! on this path shells out to a registry" stays a fact this test can observe
//! rather than one it asserts by construction. A test in `aenv-core` could not
//! install that.

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

/// The image every fixture here asks for.
///
/// Fully qualified on purpose: an unqualified name is expanded across
/// `image.resolver.search_registries` into one candidate per registry, and
/// the fake `regctl` below would then be run once per candidate. One
/// candidate makes "was a resolver reached" a single, unambiguous fact.
/// `.invalid` is reserved by RFC 6761 and can never resolve, so a fixture
/// that stopped installing the fake `regctl` would fail rather than reach
/// a real registry.
const IMAGE: &str = "registry.invalid/agentenv/cold-start:pinned";

/// One API surface, plus the place the fake `regctl` leaves its evidence.
struct Surface {
    api: Arc<ApiImpl>,
    /// `{deps_path}/regctl/{version}/`: the directory the fake `regctl` is
    /// symlinked into, and therefore the one it writes `argv` into.
    regctl_dir: PathBuf,
}

/// The surface this route is served by.
///
/// 🔴 The orchestrator takes `AccessTokenSeedPolicy::MayGenerate`: the
/// cold-start route asks the `ApiImpl`, not the orchestrator, and an
/// orchestrator built with `MustBeConfigured` has construction-time demands of
/// its own that would make this test fail on the fixture instead of on the
/// route.
///
/// 🔴 `ResumeWiring::api_half_for_test` because that is the half this route is
/// answered on: `aenv-node` replies 404 to the whole user-facing REST surface
/// (`aenv_core::api::role_gate`) and cold-creates over gRPC instead, in
/// `NodeSandboxService::create`.
async fn surface() -> Surface {
    let root = tempfile::tempdir().expect("a temp dir");

    // A fake `regctl` that answers every lookup with a registry 404. The
    // 404 matters twice over: `run_regctl` treats `[http 404]` as final and
    // skips its five-attempt retry budget, so a test that did reach it would
    // neither sleep nor spawn more than once, and `ImageError::NotFound` is a
    // *user* error, so the road not taken would end in a 400 rather than a
    // 500.
    //
    // 🔴 Symlinked from the repository rather than written out here: while
    // any thread in this process holds a write fd on an executable, every
    // concurrent `fork` inherits it and the following `execve` is refused
    // with `ETXTBSY`. Symlinking never opens the target for writing.
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

    // Held for the process's lifetime: the persister above goes on reading
    // it, and the fake `regctl` is under it too.
    std::mem::forget(root);

    Surface { api, regctl_dir }
}

fn claims() -> aenv_core::api::impls::Claims {
    aenv_core::api::impls::Claims
}

fn host() -> Host {
    Host::from(http::uri::Authority::from_static("localhost"))
}

/// `POST /sandboxes-cold`, with the one body every fixture here sends.
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

/// The argv of the fake `regctl`'s last run, or `None` if it never ran.
///
/// 🔴 This is the discriminator. "Did this call resolve the image here" is
/// answerable only as a side effect: resolution's first act is to shell out to
/// `regctl`, and the fake records what it was asked for. A status code cannot
/// answer it.
fn regctl_argv(s: &Surface) -> Option<Vec<String>> {
    std::fs::read_to_string(s.regctl_dir.join("argv"))
        .ok()
        .map(|raw| raw.lines().map(ToString::to_string).collect())
}

/// This route used to be refused outright on the deciding half — see the
/// retired history on `SandboxLaunchSource::UnresolvedImage`. This pins the
/// replacement: it touches no `regctl` (it cannot — no `/dev/kvm`, no `ublk`
/// either) but *does* build a launch source and hand it to the orchestrator,
/// carrying a reference instead of an already-resolved path.
///
/// # 🔴 Why the assertion is a refusal from `build_from_image_ref`
///
/// `surface`'s fixture orchestrator is a `MockBackendFactory` rather than a
/// `RemoteSandboxBackendFactory` dialling a real node, so an unresolved-image
/// create on this fixture runs out of road at
/// `SandboxBackendFactory::build_from_image_ref`'s *default* refusal. That
/// refusal is exactly what proves the request got there at all — reaching it
/// means `sandboxes_cold_post` built `SandboxLaunchSource::UnresolvedImage`
/// from `body.image` verbatim and handed it to the orchestrator without ever
/// calling `regctl`, which is the whole of what this route owes now. A real
/// remote dispatch — the node actually resolving the reference itself — is
/// exercised end-to-end in `node_client::tests` and `node_server::tests`,
/// which run a real node service over a real socket.
///
/// 🔴 A second arm used to drive the same request through a
/// `ResumeWiring::node_local` surface and assert that `regctl` *did* run,
/// because `sandboxes_cold_post` forked on `ApiImpl::runs_sandbox_runtime` and
/// resolved in-process on the running half. That fork is collapsed: a node
/// answers this route with 404 (`aenv_core::api::role_gate`) and resolves
/// inside `NodeSandboxService::create`'s `Source::Image` arm instead, which
/// `crates/aenv-node/src/node_server/tests.rs` covers.
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
