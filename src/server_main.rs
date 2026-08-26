//! The part of a server process that is the same on both halves.
//!
//! # 🔴 One serve loop, two binaries
//!
//! `aenv-node` and `aenv-api` assemble completely different things — one boots
//! microVMs, the other owns the cluster's ledger — but what happens *after*
//! assembly is identical: bind the HTTP listener, serve until a signal, take
//! the process down in a fixed order. That order is the one real invariant in
//! this file (see [`RUNTIME_SHUTDOWN_TIMEOUT`] and
//! [`Assembly::pg_singleton_tasks`] for the two places it has already been got
//! wrong), so it lives once, here, rather than once per binary.
//!
//! What differs is behind [`Assembly`]: the router, the orchestration surface,
//! the background tasks, and — for the half that brought up a machine —
//! whatever [`ProcessRuntime`] it has to stand down.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::serve::ListenerExt;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::observability::ObservabilityReporter;
use crate::orchestrator::SandboxOrchestration;
use crate::role::ServerRole;

/// Whatever a process brought up on the machine it runs on, and has to take
/// back down before exit.
///
/// 🔴 A trait and not a concrete type: the half that runs no sandboxes brings
/// up no Firecracker pool, no ublk daemon and no P2P endpoint, and must not
/// link the code that would. `None` on that half is the whole statement.
#[async_trait::async_trait]
pub trait ProcessRuntime: Send {
    async fn shutdown(self: Box<Self>);
}

/// What an assembled role hands back to `main`: the router it serves and the
/// four things the shutdown path has to stand down, in that order.
pub struct Assembly {
    pub app: axum::Router,
    /// The orchestration surface this process drives. Which concrete
    /// `Orchestrator` is behind it is the role's decision.
    pub orchestration: Arc<dyn SandboxOrchestration>,
    /// Background tasks to stop before the shutdown pauses start.
    pub upkeep: Vec<tokio::task::JoinHandle<()>>,
    /// PostgreSQL-elected singleton background tasks (Stage B: the catalog
    /// build reaper). Kept apart from `upkeep` deliberately —
    /// [`LeaderTaskHandle::shutdown`][crate::leader_task::LeaderTaskHandle::shutdown] is `async`, consumes
    /// `self`, and releases this replica's advisory lock (if it is
    /// currently leader) before returning; pushing one into `upkeep` and
    /// letting the shutdown loop `.abort()` it would skip that release
    /// entirely and strand the lock until the pool itself is torn down.
    /// Empty for `--role node`, which never holds a `[pg]` pool at all.
    pub pg_singleton_tasks: Vec<crate::leader_task::LeaderTaskHandle>,
    /// The heartbeat sender, for roles that report themselves as a machine.
    pub reporter: Option<ObservabilityReporter>,
    /// The machine-local runtime, for roles that brought one up.
    pub runtime: Option<Box<dyn ProcessRuntime>>,
    /// The gRPC surface this role serves alongside the HTTP one, already
    /// accepting: the task serving it, and the channel that stops it.
    ///
    /// 🔴 Already bound by the time this is built. A listener that binds inside
    /// a spawned task turns "the port is taken" into a task that quietly ended,
    /// and the process goes on serving HTTP with a gRPC surface nobody can
    /// reach — which is indistinguishable from a surface nobody is calling.
    ///
    pub grpc: Option<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>,
}

/// Bound for the final `Runtime::shutdown_timeout` call in `main`, below.
///
/// 🔴 Deliberately larger than [`NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT`]: by the
/// time this runs, every shutdown step this file knows to bound has already
/// run and already had its own timeout, so this bound is what is left over
/// for whatever *wasn't* individually bounded — a stuck `spawn_blocking`
/// nothing above reached explicitly. It only needs to be comfortably smaller
/// than Kubernetes' `terminationGracePeriodSeconds` (observed misconfigured at
/// 3600s on the cluster this exists for), not tight.
pub const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Serves `assembly` until a shutdown signal, then takes the process down in
/// the order the comments below spell out.
pub async fn serve(role: ServerRole, config: &AppConfig, assembly: Assembly) -> anyhow::Result<()> {
    let Assembly {
        app,
        orchestration,
        upkeep,
        pg_singleton_tasks,
        mut reporter,
        runtime,
        grpc,
    } = assembly;

    // Split so the stop signal can travel into the graceful-shutdown closure
    // while the task stays here to be joined after it.
    let (grpc_task, grpc_shutdown) = match grpc {
        Some((task, shutdown)) => (Some(task), Some(shutdown)),
        None => (None, None),
    };

    let addr = std::env::var("API_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
    let shutdown_orchestration = Arc::clone(&orchestration);
    let drain_orchestration = Arc::clone(&orchestration);
    let drains_on_shutdown = role.drains_on_shutdown();
    let drain_propagation =
        Duration::from_secs(config.orchestrator.shutdown_drain_propagation_secs);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // envd streams a command's lifecycle as a burst of tiny Connect-RPC frames.
    // With Nagle left on, the frame after the first one waits for the client's
    // delayed ACK, adding a ~40ms floor to every short-lived command.
    let listener = tokio::net::TcpListener::bind(&addr).await?.tap_io(|stream| {
        if let Err(err) = stream.set_nodelay(true) {
            warn!(target: "agentenv", error = %err, "failed to set TCP_NODELAY on incoming connection");
        }
    });
    info!(target: "agentenv", addr = %addr, "API server listening");

    let shutdown_cleanup = tokio::spawn(async move {
        if let Ok(()) = shutdown_rx.await {
            if let Some(mut handle) = reporter.take() {
                info!(target: "agentenv", "stopping observability reporter before process exit");
                if let Err(err) = handle.shutdown().await {
                    warn!(target: "agentenv", error = %err, "error occurred while shutting down observability reporter");
                }
            }
            // Stop reconciling before the shutdown pauses start: those write
            // paused records these tasks would otherwise be racing to inspect.
            for task in &upkeep {
                task.abort();
            }
            // 🔴 Awaited, never `.abort()`-ed — see `Assembly::pg_singleton_tasks`'s
            // own doc comment: `shutdown()` releases this replica's
            // PostgreSQL advisory lock if it is currently leader, which an
            // abort would skip entirely.
            for task in pg_singleton_tasks {
                info!(target: "agentenv", "stopping a pg-elected singleton task before process exit");
                task.shutdown().await;
            }
            info!(target: "agentenv", "stopping sandboxes before process exit");
            if let Err(err) = shutdown_orchestration.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down orchestrator");
            }
            if let Some(runtime) = runtime {
                runtime.shutdown().await;
            }
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;

            // Take the node out of rotation before tearing anything down, and
            // give the scheduler time to hear about it. Without this pause a
            // sandbox can be placed here in the moments after the signal and be
            // paused again before the caller has finished starting it.
            //
            // Isolation set through the admin API is left alone: it is already
            // the state we want, and re-announcing it would reset the timestamp
            // an operator is watching.
            //
            // 🔴 Only a role that can be scheduled onto has anything to
            // withdraw; an API replica is not a placement target.
            if drains_on_shutdown && drain_orchestration.set_scheduling_disabled(true) {
                info!(
                    target: "agentenv",
                    wait_secs = drain_propagation.as_secs(),
                    "isolated the node for shutdown; waiting for the scheduler to notice"
                );
                if !drain_propagation.is_zero() {
                    tokio::time::sleep(drain_propagation).await;
                }
            }

            // Stops accepting on the second listener at the same moment the
            // HTTP one stops: both carry work that the teardown below is about
            // to make impossible to finish.
            if let Some(shutdown) = grpc_shutdown {
                let _ = shutdown.send(());
            }

            let _ = shutdown_tx.send(());
        })
        .await?;

    if let Some(task) = grpc_task {
        if let Err(err) = task.await {
            warn!(target: "agentenv", error = %err, "the gRPC surface did not stop cleanly");
        }
    }

    shutdown_cleanup.await?;

    Ok(())
}

/// Binds a gRPC listener and spawns the server that answers on it.
///
/// 🔴 The bind happens here, in the assembly, and not inside the spawned task.
/// A `serve(addr, ..)` that binds inside its own future turns "the port is
/// already in use" into a task that ended: the process goes on serving HTTP,
/// the surface is unreachable, and from the outside that is the same picture as
/// a surface nobody is calling. See §15.4 ③ — a reading of zero that means two
/// different things.
pub async fn spawn_grpc_surface<F, Fut>(
    addr: &str,
    surface: &'static str,
    serve: F,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>
where
    F: FnOnce(tokio::net::TcpListener, GrpcShutdown) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind the {surface} to {addr}"))?;
    let bound = listener.local_addr().ok();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = serve(
        listener,
        Box::pin(async move {
            let _ = shutdown_rx.await;
        }),
    );
    let task = tokio::spawn(async move {
        if let Err(err) = serving.await {
            // 🔴 `error`, not `warn`. Reaching here means the surface stopped
            // answering while the process kept running, which is the state this
            // whole arrangement exists to make impossible to reach quietly.
            tracing::error!(
                target: "agentenv",
                surface,
                error = %format_args!("{err:#}"),
                "a gRPC surface stopped serving"
            );
        }
    });
    info!(target: "agentenv", surface, addr = ?bound, "gRPC surface listening");
    Ok((task, shutdown_tx))
}

/// The stop signal a gRPC surface waits on.
///
/// Boxed so that [`spawn_grpc_surface`] can hand the same concrete type to
/// every `serve_on`, each of which takes an opaque `impl Future`.
pub type GrpcShutdown = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!(target: "agentenv", "received Ctrl+C, starting graceful shutdown");
        }
        _ = terminate => {
            info!(target: "agentenv", "received SIGTERM, starting graceful shutdown");
        }
    }
}
