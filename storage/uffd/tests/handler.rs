//! The handler against a userfaultfd this process created, playing the VMM.

use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use storage_util::io_ring::AsyncIoRing;
use uvm_uffd::proto::{UFFD_FEATURE_EVENT_REMOVE, UFFD_USER_MODE_ONLY};
use uvm_uffd::testing::{create_uffd_for_test, AnonRegion};
use uvm_uffd::{
    send_handshake, HandlerOptions, HandlerState, LocalBoxFuture, MemSource, PageSource, Uffd,
    UffdHandler,
};

const PAGE: usize = 4096;
const MIB: usize = 1 << 20;

fn uffd_or_skip(test: &str) -> Option<Uffd> {
    if let Some((uffd, _)) = create_uffd_for_test() {
        return Some(uffd);
    }
    let reason = "cannot create a userfaultfd (needs CAP_SYS_PTRACE, vm.unprivileged_userfaultfd=1 or /dev/userfaultfd)";
    if std::env::var("AENV_UFFD_TEST_REQUIRED").as_deref() == Ok("1") {
        panic!("AENV_UFFD_TEST_REQUIRED=1 but {test} cannot run: {reason}");
    }
    eprintln!("SKIPPED[uffd]: {test} ({reason})");
    let _ = UFFD_USER_MODE_ONLY;
    None
}

fn opts(name: &str) -> HandlerOptions {
    HandlerOptions {
        max_inflight: 16,
        read_retry_budget: Duration::from_secs(5),
        handshake_timeout: Duration::from_secs(5),
        drain_timeout: Duration::from_millis(200),
        name: name.to_string(),
    }
}

fn wait_for<F: FnMut() -> bool>(mut cond: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

#[test]
fn pages_come_from_the_source() -> Result<()> {
    let Some(uffd) = uffd_or_skip("pages_come_from_the_source") else {
        return Ok(());
    };
    let size = 8 * MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("source"),
    )?;

    let pages = size / PAGE;
    for p in [0usize, 1, 7, 100, pages - 1, 512, 3, 1023] {
        let got = region.read(p * PAGE, PAGE);
        assert_eq!(
            &got[..],
            &source.as_slice()[p * PAGE..(p + 1) * PAGE],
            "page {p}"
        );
    }
    // A second read of a served page does not fault.
    let before = handler.stats().faults;
    let _ = region.read_byte(7 * PAGE + 9);
    assert_eq!(handler.stats().faults, before);

    let stats = handler.stats();
    assert_eq!(stats.pages_copied, 8);
    assert_eq!(stats.bytes_read, 8 * PAGE as u64);
    assert_eq!(handler.state(), HandlerState::Serving);
    handler.stop()?;
    Ok(())
}

#[test]
fn zero_pages_are_installed_without_a_copy() -> Result<()> {
    let Some(uffd) = uffd_or_skip("zero_pages_are_installed_without_a_copy") else {
        return Ok(());
    };
    let size = 2 * MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    // Every fourth page is zero; the image is one page short of the region,
    // so the last page lies past it.
    let source = Arc::new(MemSource::patterned(size - PAGE, PAGE, 4));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("zero"),
    )?;

    let pages = size / PAGE;
    let mut zero_pages = 0;
    for p in 0..pages {
        let got = region.read(p * PAGE, PAGE);
        if p == pages - 1 {
            assert!(
                got.iter().all(|b| *b == 0),
                "the page past the image is zero"
            );
            zero_pages += 1;
        } else {
            assert_eq!(
                &got[..],
                &source.as_slice()[p * PAGE..(p + 1) * PAGE],
                "page {p}"
            );
            if p % 4 == 0 {
                zero_pages += 1;
            }
        }
    }
    let stats = handler.stats();
    assert_eq!(stats.pages_zeroed, zero_pages);
    assert_eq!(stats.pages_copied + stats.pages_zeroed, pages as u64);
    // The page past the image was never read.
    assert_eq!(stats.bytes_read, (pages as u64 - 1) * PAGE as u64);
    handler.stop()?;
    Ok(())
}

#[test]
fn concurrent_faulters_on_the_same_pages_install_each_page_once() -> Result<()> {
    let Some(uffd) = uffd_or_skip("concurrent_faulters_on_the_same_pages_install_each_page_once")
    else {
        return Ok(());
    };
    let size = 4 * MIB;
    let region = Arc::new(AnonRegion::new(size)?);
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("concurrent"),
    )?;

    let pages = size / PAGE;
    let workers: Vec<_> = (0..8)
        .map(|t| {
            let region = Arc::clone(&region);
            let source = Arc::clone(&source);
            std::thread::spawn(move || {
                // Every thread walks every page, half of them backwards.
                let order: Vec<usize> = if t % 2 == 0 {
                    (0..pages).collect()
                } else {
                    (0..pages).rev().collect()
                };
                for p in order {
                    let got = region.read(p * PAGE, PAGE);
                    assert_eq!(
                        &got[..],
                        &source.as_slice()[p * PAGE..(p + 1) * PAGE],
                        "page {p}"
                    );
                }
            })
        })
        .collect();
    for w in workers {
        w.join().expect("faulting thread");
    }
    let stats = handler.stats();
    assert_eq!(stats.pages_copied, pages as u64, "{stats:?}");
    assert_eq!(handler.state(), HandlerState::Serving);
    handler.stop()?;
    Ok(())
}

#[test]
fn a_removed_page_reads_back_as_zeros() -> Result<()> {
    let Some(uffd) = uffd_or_skip("a_removed_page_reads_back_as_zeros") else {
        return Ok(());
    };
    let size = MIB;
    let region = AnonRegion::new(size)?;
    let granted = region.register(&uffd, UFFD_FEATURE_EVENT_REMOVE)?;
    if granted & UFFD_FEATURE_EVENT_REMOVE == 0 {
        eprintln!("SKIPPED[uffd]: a_removed_page_reads_back_as_zeros (no EVENT_REMOVE)");
        return Ok(());
    }
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("remove"),
    )?;

    let p = 5;
    assert_eq!(
        region.read_byte(p * PAGE + 1),
        source.as_slice()[p * PAGE + 1]
    );
    region.write_byte(p * PAGE + 1, 0xEE);
    region.discard(p * PAGE, PAGE)?;
    assert!(
        wait_for(|| handler.stats().removes >= 1, Duration::from_secs(5)),
        "the REMOVE event reached the handler"
    );
    let got = region.read(p * PAGE, PAGE);
    assert!(
        got.iter().all(|b| *b == 0),
        "a discarded page is zero, not refilled"
    );
    let stats = handler.stats();
    assert_eq!(stats.pages_copied, 1);
    assert_eq!(stats.pages_zeroed, 1);
    // An untouched page is still served from the image.
    assert_eq!(
        region.read_byte(9 * PAGE + 3),
        source.as_slice()[9 * PAGE + 3]
    );
    handler.stop()?;
    Ok(())
}

/// A source whose reads never complete; the fault it serves stays blocked.
struct HangingSource {
    size: u64,
    entered: AtomicBool,
}

impl PageSource for HangingSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_page<'a>(
        &'a self,
        _ring: &'a AsyncIoRing,
        _offset: u64,
        _dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}

#[test]
fn stop_releases_a_faulter_the_source_left_blocked() -> Result<()> {
    let Some(uffd) = uffd_or_skip("stop_releases_a_faulter_the_source_left_blocked") else {
        return Ok(());
    };
    let size = MIB;
    let region = Arc::new(AnonRegion::new(size)?);
    region.register(&uffd, 0)?;
    let source = Arc::new(HangingSource {
        size: size as u64,
        entered: AtomicBool::new(false),
    });
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("hang"),
    )?;

    let faulter = {
        let region = Arc::clone(&region);
        std::thread::spawn(move || region.read_byte(3 * PAGE))
    };
    assert!(
        wait_for(
            || source.entered.load(Ordering::SeqCst),
            Duration::from_secs(5)
        ),
        "the read started"
    );
    assert!(!faulter.is_finished(), "the faulter is blocked on the read");
    handler.stop()?;
    // Closing the descriptor lets the kernel finish the fault with a plain
    // anonymous page.
    let started = Instant::now();
    let byte = faulter.join().expect("faulting thread");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(byte, 0);
    Ok(())
}

struct FailingSource {
    size: u64,
}

impl PageSource for FailingSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_page<'a>(
        &'a self,
        _ring: &'a AsyncIoRing,
        offset: u64,
        _dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { bail!("backend unreachable at {offset}") })
    }
}

#[test]
fn a_read_that_keeps_failing_ends_the_handler_after_its_budget() -> Result<()> {
    let Some(uffd) = uffd_or_skip("a_read_that_keeps_failing_ends_the_handler_after_its_budget")
    else {
        return Ok(());
    };
    let size = MIB;
    let region = Arc::new(AnonRegion::new(size)?);
    region.register(&uffd, 0)?;
    let source = Arc::new(FailingSource { size: size as u64 });
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        source,
        HandlerOptions {
            read_retry_budget: Duration::from_millis(300),
            ..opts("failing")
        },
    )?;
    let faulter = {
        let region = Arc::clone(&region);
        std::thread::spawn(move || region.read_byte(2 * PAGE))
    };
    assert!(
        wait_for(
            || matches!(handler.state(), HandlerState::Exited(Some(_))),
            Duration::from_secs(5)
        ),
        "the handler exited with an error: {:?}",
        handler.state()
    );
    let stats = handler.stats();
    assert!(stats.read_retries >= 2, "{stats:?}");
    let err = handler.stop().expect_err("stop reports the failure");
    assert!(err.to_string().contains("backend unreachable"), "{err:#}");
    assert_eq!(faulter.join().expect("faulting thread"), 0);
    Ok(())
}

#[test]
fn the_socket_handshake_carries_the_descriptor_and_the_mappings() -> Result<()> {
    let Some(uffd) = uffd_or_skip("the_socket_handshake_carries_the_descriptor_and_the_mappings")
    else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("uffd.sock");
    let listener = UnixListener::bind(&socket)?;
    let size = 2 * MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_socket(listener, Arc::clone(&source), opts("socket"))?;
    assert_eq!(handler.state(), HandlerState::Starting);

    // The image offset of the region is not zero, as with a second guest
    // memory region.
    let image_offset = 64 * PAGE as u64;
    let source_for_region = Arc::new(MemSource::new({
        let mut data = vec![0u8; image_offset as usize];
        data.extend_from_slice(source.as_slice());
        data
    }));
    drop(handler);
    let listener = UnixListener::bind(dir.path().join("uffd2.sock"))?;
    let socket = dir.path().join("uffd2.sock");
    let handler =
        UffdHandler::serve_socket(listener, Arc::clone(&source_for_region), opts("socket"))?;

    let stream = UnixStream::connect(&socket)?;
    let mappings = vec![region.mapping(image_offset, PAGE as u64)];
    send_handshake(
        &stream,
        &mappings,
        &[std::os::fd::AsRawFd::as_raw_fd(&uffd)],
    )?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), handler.wait_serving()).await
    })??;
    // Our copy of the descriptor is closed; the handler owns its own.
    drop(uffd);

    for p in [0usize, 3, 400] {
        let got = region.read(p * PAGE, PAGE);
        assert_eq!(
            &got[..],
            &source.as_slice()[p * PAGE..(p + 1) * PAGE],
            "page {p}"
        );
    }
    handler.stop()?;
    Ok(())
}

#[test]
fn no_handshake_within_the_timeout_ends_the_handler() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let listener = UnixListener::bind(dir.path().join("uffd.sock"))?;
    let source = Arc::new(MemSource::patterned(MIB, PAGE, 0));
    let handler = UffdHandler::serve_socket(
        listener,
        source,
        HandlerOptions {
            handshake_timeout: Duration::from_millis(200),
            ..opts("timeout")
        },
    )?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let err = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), handler.wait_exit()).await
    })?;
    assert!(
        err.as_deref().unwrap_or("").contains("no uffd handshake"),
        "{err:?}"
    );
    assert!(handler.stop().is_err());
    Ok(())
}
