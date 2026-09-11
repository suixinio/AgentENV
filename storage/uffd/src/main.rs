use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use uvm_uffd::testing::{create_uffd_for_test, AnonRegion};
use uvm_uffd::{
    FileSource, HandlerOptions, MemSource, OverlaybdSource, PageSource, PrefetchList,
    StatsSnapshot, UffdHandler,
};

#[derive(Debug, Parser)]
#[command(
    name = "uvm-uffd",
    about = "userfaultfd page server for Firecracker memory restore"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve an overlaybd memory image to the VM that connects to the socket.
    Serve {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        image_config: PathBuf,
        #[arg(long)]
        global_config: PathBuf,
        #[arg(long, default_value_t = 64)]
        max_inflight: usize,
        #[command(flatten)]
        prefetch: PrefetchArgs,
    },
    /// Serve a raw memory file to the VM that connects to the socket.
    ServeFile {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 64)]
        max_inflight: usize,
        #[command(flatten)]
        prefetch: PrefetchArgs,
    },
    /// Fault an in-memory image through this process's own userfaultfd and
    /// report the fault rate.
    Selftest {
        #[arg(long, default_value_t = 256)]
        size_mib: usize,
        #[arg(long, default_value_t = 64)]
        max_inflight: usize,
        #[arg(long, default_value_t = 4)]
        threads: usize,
    },
}

#[derive(Debug, Default, clap::Args)]
struct PrefetchArgs {
    /// Replay this working-set list once the handshake is done.
    #[arg(long)]
    prefault: Option<PathBuf>,
    /// Record the working set here at exit, unless the file exists.
    #[arg(long)]
    record_prefetch: Option<PathBuf>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build the tokio runtime")?;
    match cli.command {
        Command::Serve {
            socket,
            image_config,
            global_config,
            max_inflight,
            prefetch,
        } => runtime.block_on(async move {
            let source = OverlaybdSource::open(&global_config, &image_config)
                .await
                .context("open the overlaybd memory image")?;
            serve(socket, Arc::new(source), max_inflight, prefetch).await
        }),
        Command::ServeFile {
            socket,
            file,
            max_inflight,
            prefetch,
        } => runtime.block_on(async move {
            let source = FileSource::open(&file)?;
            serve(socket, Arc::new(source), max_inflight, prefetch).await
        }),
        Command::Selftest {
            size_mib,
            max_inflight,
            threads,
        } => selftest(size_mib, max_inflight, threads),
    }
}

async fn serve<S: PageSource>(
    socket: PathBuf,
    source: Arc<S>,
    max_inflight: usize,
    prefetch: PrefetchArgs,
) -> Result<()> {
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("bind the handshake socket {}", socket.display()))?;
    let opts = HandlerOptions {
        max_inflight,
        ..HandlerOptions::default()
    };
    let image_size = source.size();
    let handler = UffdHandler::serve_socket(listener, source, opts)?;
    println!("{}", socket.display());
    println!("ready");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install the SIGTERM handler")?;
    let replay = async {
        let Some(path) = prefetch.prefault.as_deref() else {
            return Ok(());
        };
        let Some(list) = PrefetchList::read(path)? else {
            return Ok(());
        };
        handler.wait_serving().await?;
        let page_size = handler.page_size().unwrap_or(0);
        match list.pages_for(page_size, image_size) {
            Some(pages) => {
                eprintln!("prefaulting {} pages from {}", pages.len(), path.display());
                handler.prefault(pages.to_vec())
            }
            None => {
                eprintln!(
                    "ignoring {}: recorded for page size {} and image size {}, serving {page_size} and {image_size}",
                    path.display(),
                    list.page_size,
                    list.image_size
                );
                Ok(())
            }
        }
    };
    let exited = async {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = sigterm.recv() => Ok(()),
            err = handler.wait_exit() => match err {
                Some(err) => Err(anyhow::anyhow!("uffd handler failed: {err}")),
                None => Ok(()),
            },
        }
    };
    let (replayed, exit) = tokio::join!(replay, exited);
    replayed?;
    let stats = handler.stats();
    if let Some(path) = prefetch.record_prefetch.as_deref() {
        if let Some(page_size) = handler.page_size() {
            let list = PrefetchList::new(page_size, image_size, handler.faulted_pages());
            if list.is_worth_recording() && list.write_if_absent(path)? {
                eprintln!("recorded {} pages to {}", list.pages.len(), path.display());
            }
        }
    }
    let result = tokio::task::spawn_blocking(move || handler.stop())
        .await
        .context("join the uffd handler")?;
    print_stats(&stats);
    let _ = std::fs::remove_file(&socket);
    exit.and(result)
}

fn print_stats(stats: &StatsSnapshot) {
    println!(
        "faults={} copied={} zeroed={} present={} duplicates={} bytes_read={} read_retries={} copy_retries={} removes={} prefaulted={} unmapped={}",
        stats.faults,
        stats.pages_copied,
        stats.pages_zeroed,
        stats.already_present,
        stats.duplicates,
        stats.bytes_read,
        stats.read_retries,
        stats.copy_retries,
        stats.removes,
        stats.prefaulted,
        stats.unmapped
    );
}

fn selftest(size_mib: usize, max_inflight: usize, threads: usize) -> Result<()> {
    const PAGE: usize = 4096;
    let size = size_mib << 20;
    let Some((uffd, _)) = create_uffd_for_test() else {
        bail!("cannot create a userfaultfd: needs CAP_SYS_PTRACE, vm.unprivileged_userfaultfd=1 or access to /dev/userfaultfd");
    };
    let region = Arc::new(AnonRegion::new(size)?);
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 16));
    let mappings = vec![region.mapping(0, PAGE as u64)];
    let opts = HandlerOptions {
        max_inflight,
        name: "selftest".to_string(),
        ..HandlerOptions::default()
    };
    let handler = UffdHandler::serve_fd(uffd.into_owned_fd(), mappings, Arc::clone(&source), opts)?;

    let pages = size / PAGE;
    let per_thread = pages.div_ceil(threads.max(1));
    let started = Instant::now();
    let workers: Vec<_> = (0..threads.max(1))
        .map(|t| {
            let region = Arc::clone(&region);
            std::thread::spawn(move || {
                let first = t * per_thread;
                let last = ((t + 1) * per_thread).min(pages);
                let mut sum = 0u64;
                for p in first..last {
                    sum += u64::from(region.read_byte(p * PAGE + 17));
                }
                sum
            })
        })
        .collect();
    for w in workers {
        w.join().expect("selftest thread");
    }
    let elapsed = started.elapsed();

    for p in (0..pages).step_by(97) {
        let got = region.read(p * PAGE, PAGE);
        let want = &source.as_slice()[p * PAGE..(p + 1) * PAGE];
        if got != want {
            bail!("page {p} differs from the source");
        }
    }
    let stats = handler.stats();
    handler.stop()?;
    print_stats(&stats);
    let per_fault = if stats.faults > 0 {
        elapsed / stats.faults as u32
    } else {
        Duration::ZERO
    };
    println!(
        "pages={pages} threads={threads} elapsed={elapsed:?} faults_per_s={:.0} per_fault={per_fault:?}",
        stats.faults as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
