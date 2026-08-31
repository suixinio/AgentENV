//! Shared HTTP serving and ordered shutdown for API and node binaries.
//!
//! Assembly differs by binary; listener lifecycle and teardown order remain common.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::serve::ListenerExt;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::observability::ObservabilityReporter;
use crate::orchestrator::SandboxOrchestration;

/// Machine-local process runtime that must be shut down before exit.
#[async_trait::async_trait]
pub trait ProcessRuntime: Send {
    async fn shutdown(self: Box<Self>);
}

/// Router and ordered shutdown resources returned by binary assembly.
pub struct Assembly {
    pub app: axum::Router,
    /// Orchestration surface selected by the assembling binary.
    pub orchestration: Arc<dyn SandboxOrchestration>,
    /// Background tasks to stop before the shutdown pauses start.
    pub upkeep: Vec<tokio::task::JoinHandle<()>>,
    /// PostgreSQL-elected tasks shut down gracefully to release advisory locks.
    pub pg_singleton_tasks: Vec<crate::leader_task::LeaderTaskHandle>,
    /// The heartbeat sender, for a process that reports itself as a machine.
    pub reporter: Option<ObservabilityReporter>,
    /// The machine-local runtime, for a process that brought one up.
    pub runtime: Option<Box<dyn ProcessRuntime>>,
    /// Whether this process withdraws itself from placement during shutdown.
    pub drains_on_shutdown: bool,
    /// Already-bound gRPC task and its shutdown sender.
    pub grpc: Option<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>,
}

/// Final Tokio runtime shutdown bound after individually bounded cleanup.
pub const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Serves until a signal, then tears down assembly in order.
pub async fn serve(config: &AppConfig, assembly: Assembly) -> anyhow::Result<()> {
    let Assembly {
        app,
        orchestration,
        upkeep,
        pg_singleton_tasks,
        mut reporter,
        runtime,
        drains_on_shutdown,
        grpc,
    } = assembly;

    // Separate the gRPC stop signal from the task joined after HTTP shutdown.
    let (grpc_task, grpc_shutdown) = match grpc {
        Some((task, shutdown)) => (Some(task), Some(shutdown)),
        None => (None, None),
    };

    let addr = std::env::var("API_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
    let shutdown_orchestration = Arc::clone(&orchestration);
    let drain_orchestration = Arc::clone(&orchestration);
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
            // Stop reconciliation before shutdown pauses mutate the same records.
            for task in &upkeep {
                task.abort();
            }
            // Graceful shutdown releases each singleton task's advisory lock.
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

            // Withdraw schedulable nodes before teardown and allow propagation.
            // Preserve pre-existing operator isolation timestamps.
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

            // Stop accepting gRPC work before teardown makes it impossible.
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

/// Binds before spawning so listener failures remain startup failures.
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
            // A serving task stopping while the process runs is an error.
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

/// Boxed stop signal accepted by every gRPC `serve_on` implementation.
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
