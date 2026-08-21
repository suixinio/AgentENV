use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=services/api/proto/scheduler.proto");
    println!("cargo:rerun-if-changed=services/api/proto/node.proto");
    emit_git_rerun_inputs();

    // The server stubs exist for the in-process fake controller the registry
    // client's transport tests run against. That client's whole job is turning
    // gRPC outcomes into the right kind of failure, and the outcomes worth
    // testing — a deadline, a refused connection, a status that sounds like an
    // answer — can only be produced by something on the other end of a socket.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "services/api/proto/scheduler.proto",
                // 🔴 Deliberately not added to `services/Makefile`'s explicit
                // proto list: both ends of the node service are Rust, and a
                // `.pb.go` nobody imports is one more generated artifact to
                // keep in step.
                "services/api/proto/node.proto",
            ],
            &["services/api/proto"],
        )
        .expect("failed to compile the scheduler and node protos for Rust gRPC");

    let commit = resolve_git_commit().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=AENV_GIT_COMMIT={commit}");
}

fn emit_git_rerun_inputs() {
    if let Some(path) = git_path("HEAD") {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let Some(head_ref) = git_stdout(["symbolic-ref", "-q", "HEAD"]) else {
        return;
    };
    let head_ref = head_ref.trim();
    if head_ref.is_empty() {
        return;
    }

    if let Some(path) = git_path(head_ref) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    // Track packed refs as well as the loose branch ref so changes after
    // repacking or worktree-local HEAD updates still refresh the embedded SHA.
    if let Some(path) = git_path("packed-refs").filter(|path| path.exists()) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_path(path: &str) -> Option<PathBuf> {
    git_stdout(["rev-parse", "--git-path", path]).map(|path| PathBuf::from(path.trim()))
}

fn resolve_git_commit() -> Option<String> {
    let commit = git_stdout(["rev-parse", "--short", "HEAD"])?;
    let commit = commit.trim();
    (!commit.is_empty()).then(|| commit.to_string())
}

fn git_stdout<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout).ok()
}
