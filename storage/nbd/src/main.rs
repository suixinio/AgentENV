use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use uvm_nbd::{MemTarget, NbdDevice, NbdOptions, OverlaybdTarget};

#[derive(Debug, Parser)]
#[command(
    name = "uvm-nbd",
    about = "Expose a userspace block target as /dev/nbdN through the kernel nbd driver."
)]
struct Cli {
    /// Log level: off, error, warn, info, debug, trace.
    #[arg(long, default_value = "info", global = true)]
    log_level: tracing::level_filters::LevelFilter,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve an overlaybd image until SIGINT or SIGTERM.
    Expose {
        /// Path to the overlaybd global config JSON.
        #[arg(long)]
        global_config: PathBuf,
        /// Path to the overlaybd image config JSON.
        #[arg(long)]
        image_config: PathBuf,
        #[command(flatten)]
        device: DeviceArgs,
        /// Expose the image read-only even when its upper is writable.
        #[arg(long)]
        read_only: bool,
    },
    /// Disconnect a device this or another server left configured.
    Disconnect {
        /// The `/dev/nbdN` index.
        #[arg(long)]
        index: u32,
    },
    /// Serve an in-memory target of `--size-mib` until SIGINT or SIGTERM.
    ExposeMem {
        /// Capacity in MiB.
        #[arg(long)]
        size_mib: u64,
        /// Logical block size in bytes; a power of two in [512, 4096].
        #[arg(long, default_value_t = 4096)]
        block_size: u32,
        #[command(flatten)]
        device: DeviceArgs,
    },
}

#[derive(Debug, clap::Args)]
struct DeviceArgs {
    /// Sockets per device; each becomes a kernel hardware queue.
    #[arg(long, default_value_t = 4)]
    connections: u16,
    /// Kernel request timeout in seconds.
    #[arg(long, default_value_t = 90)]
    io_timeout_secs: u64,
    /// Requests one connection dispatches concurrently.
    #[arg(long, default_value_t = 64)]
    queue_depth: usize,
}

impl DeviceArgs {
    fn options(&self) -> NbdOptions {
        NbdOptions {
            connections: self.connections,
            io_timeout: Duration::from_secs(self.io_timeout_secs),
            queue_depth: self.queue_depth,
            ..Default::default()
        }
    }
}

/// The device path on stdout, flushed, so a caller can pipe it into fio.
fn announce(device: &NbdDevice) -> Result<()> {
    println!("{}", device.device_path().display());
    std::io::stdout().flush().context("flush the device path")
}

async fn await_signal() -> Result<()> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install the SIGTERM handler")?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("install the SIGINT handler")?;
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("received SIGTERM"),
        _ = sigint.recv() => tracing::info!("received SIGINT"),
    }
    Ok(())
}

async fn serve(device: NbdDevice) -> Result<()> {
    announce(&device)?;
    let index = device.index();
    await_signal().await?;
    tracing::info!(index, "disconnecting");
    device.stop().await
}

fn setup_tracing(level: tracing::level_filters::LevelFilter) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(format!("uvm_nbd={level}")));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init();
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    setup_tracing(cli.log_level);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    runtime.block_on(async {
        match cli.command {
            Command::Expose {
                global_config,
                image_config,
                device,
                read_only,
            } => {
                let target = OverlaybdTarget::open(&global_config, &image_config)
                    .await
                    .context("open the overlaybd target")?;
                if read_only {
                    target.set_read_only();
                }
                let target = Arc::new(target);
                let nbd = NbdDevice::start(target, device.options())
                    .await
                    .context("expose the overlaybd image")?;
                serve(nbd).await
            }
            Command::Disconnect { index } => {
                uvm_nbd::NbdNetlink::open()?
                    .disconnect(index)
                    .with_context(|| format!("disconnect nbd{index}"))?;
                tracing::info!(index, "disconnected");
                Ok(())
            }
            Command::ExposeMem {
                size_mib,
                block_size,
                device,
            } => {
                let bytes = usize::try_from(size_mib * 1024 * 1024)
                    .context("--size-mib does not fit this address space")?;
                let target = Arc::new(MemTarget::new(bytes, block_size));
                let nbd = NbdDevice::start(target, device.options())
                    .await
                    .context("expose the memory target")?;
                serve(nbd).await
            }
        }
    })
}
