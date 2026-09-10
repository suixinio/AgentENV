use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use std::io::Write;
use std::path::PathBuf;
use tracing_log::log::LevelFilter;

use storage_util::io_ring::spawn_io_ring_worker;
use uvm_ublk::{
    delete_dev, setup_tracing, wait_for_ublk_dev, MemTarget, OverlaybdTarget,
    OverlaybdTargetConfig, UVMUblkCtrlBuilder, UVMUblkDevBuilder, UVMUblkTarget,
};

#[derive(Debug, Subcommand)]
enum Device {
    Overlaybd(OverlaybdTargetConfig),
    /// An in-memory device, for measuring the transport without an image
    /// underneath it.
    Mem(MemTargetConfig),
}

#[derive(Debug, Args)]
struct MemTargetConfig {
    /// Capacity in MiB.
    #[arg(long)]
    size_mib: u64,
    /// Logical block size in bytes; a power of two in [512, 4096].
    #[arg(long, default_value_t = 4096)]
    block_size: u32,
}

#[derive(Debug, Parser)]
#[command(about = "create or delete the uvm ublk device.")]
enum Command {
    /// Create a new device, the current process will start to serve the IO request.
    #[clap(visible_alias("add"))]
    Create {
        #[command(flatten)]
        args: CreateArgs,
        #[command(subcommand)]
        device: Device,
    },
    /// Delete the device with specific id.
    #[clap(visible_alias("del"))]
    Delete {
        /// The device id that want to delete.
        dev_id: u32,
    },
    /// Recovery a previously started but stopped device.
    /// (This is a future feature, has not been implemented yet).
    Recovery,
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// The number of queues, about this device. Each queue will corresponds to
    /// a worker thread and an io uring.
    #[arg(long, default_value_t = 1)]
    nr_queues: u16,
    /// The depth of the queues, which is the concurrency within each queue.
    #[arg(long, default_value_t = 16)]
    depth: u16,
    /// The max io buffer for each slot with the queue. For example, if the
    /// depth is 16, and io_buf_size_kb is 256, then each queue will allocate
    /// 4 MB buffer, 256 KB for each slot within the queue.
    #[arg(long, default_value_t = 256)]
    io_buf_size_kb: u32,
    /// The log level, possible values includes: "off", "error", "warn", "info", "debug", "trace".
    #[arg(long, default_value = "info")]
    log_level: LevelFilter,
    /// Store the path to the pid file of the device.
    #[arg(long)]
    pid_file: Option<PathBuf>,
    /// The device id. Omit it to let the kernel pick a free one; naming it
    /// makes conflict the caller's problem (if `dev_id` already exists, the
    /// process will exit). It is an option rather than a positional because a
    /// positional before a subcommand cannot be optional.
    #[arg(long)]
    dev_id: Option<u32>,
}

async fn create_device(args: CreateArgs, device: Device) -> Result<()> {
    let (ctrl_ring, _) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
    let mut builder = UVMUblkCtrlBuilder::new()
        .nr_queues(args.nr_queues)
        .depth(args.depth)
        .max_io_buf_bytes(args.io_buf_size_kb * 1024)
        .name("overlaybd-blk");
    if let Some(dev_id) = args.dev_id {
        builder = builder.dev_id(dev_id);
    }
    let ctrl = builder.build(ctrl_ring.clone()).context("build ctrl")?;
    match device {
        Device::Overlaybd(dev_args) => {
            let tgt = OverlaybdTarget::from_config(&dev_args)
                .await
                .context("create overlaybd target")?;
            serve(ctrl_ring, ctrl, tgt).await
        }
        Device::Mem(dev_args) => {
            let bytes = usize::try_from(dev_args.size_mib * 1024 * 1024)
                .context("--size-mib does not fit this address space")?;
            let tgt = MemTarget::new(bytes, dev_args.block_size).context("create mem target")?;
            serve(ctrl_ring, ctrl, tgt).await
        }
    }
}

/// Start the device, announce its path, and serve until SIGINT or SIGTERM.
async fn serve<T: UVMUblkTarget>(
    ctrl_ring: storage_util::io_ring::IoRingHandle<io_uring::squeue::Entry128>,
    ctrl: uvm_ublk::UVMUblkCtrl,
    target: T,
) -> Result<()> {
    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;
    dev.start().await.context("start ublk dev")?;
    let dev_id = dev.dev_id();
    wait_for_ublk_dev(dev_id).context("wait for the ublkb device to show up")?;

    // The path is the first line so a caller can pipe it straight into fio;
    // `ready` stays after it for anyone watching for that instead.
    println!("{}", dev.device_path().display());
    println!("ready");
    std::io::stdout().flush().context("flush the device path")?;

    tokio::select! {
        _ = await_signal() => {}
        _ = dev.wait_for_bg_tasks() => {
            tracing::warn!(dev_id, "every queue worker exited before a signal arrived");
        }
    }

    tracing::info!(dev_id, "stopping the ublk device");
    if let Err(err) = dev.ctrl.stop_dev().await {
        tracing::warn!(dev_id, ?err, "failed to stop the ublk device before delete");
    }
    // The device holds an open fd to the ublk char dev; DEL_DEV blocks until
    // it is closed.
    drop(dev);
    delete_dev(ctrl_ring, dev_id)
        .await
        .with_context(|| format!("delete ublk device {dev_id}"))
}

async fn await_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install the SIGTERM handler");
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install the SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("received SIGTERM"),
        _ = sigint.recv() => tracing::info!("received SIGINT"),
    }
}

fn main() -> Result<()> {
    //
    // NOTE: why we use fork:
    // We need a notification method for user (the one that spawn uvm-ublk binary) that the devices
    // has prepared. Of course, the caller can poll the existence of /dev/ublkb<id>, but that might
    // not be a good idea.
    // When this process exited, the block device should be ready.
    let cmd = Command::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    rt.block_on(async {
        match cmd {
            Command::Create { args, device } => {
                setup_tracing(None, args.log_level).context("setup tracing log")?;
                create_device(args, device).await
            }
            Command::Delete { dev_id } => {
                setup_tracing(None, LevelFilter::Info).context("setup tracing log")?;
                let (ring, _) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
                delete_dev(ring, dev_id).await
            }
            Command::Recovery => {
                unimplemented!();
            }
        }
    })
}
