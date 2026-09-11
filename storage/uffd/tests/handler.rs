//! The handler against a userfaultfd this process created, playing the VMM.

use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use storage_util::io_ring::AsyncIoRing;
use uvm_uffd::proto::{
    UFFDIO_REGISTER_MODE_MISSING, UFFDIO_REGISTER_MODE_WP, UFFD_FEATURE_EVENT_REMOVE,
    UFFD_FEATURE_MISSING_HUGETLBFS, UFFD_FEATURE_PAGEFAULT_FLAG_WP, UFFD_USER_MODE_ONLY,
};
use uvm_uffd::testing::{create_uffd_for_test, AnonRegion};
use uvm_uffd::{dirty_ranges, DirtyRange, DirtySource};
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
        read_timeout: Duration::from_secs(30),
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
    assert_eq!(handler.write_protect(), Some(false));
    assert_eq!(handler.regions(), vec![region.mapping(0, PAGE as u64)]);
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

#[test]
fn faulted_pages_report_the_working_set_and_prefault_installs_ahead() -> Result<()> {
    let Some(uffd) =
        uffd_or_skip("faulted_pages_report_the_working_set_and_prefault_installs_ahead")
    else {
        return Ok(());
    };
    let size = 2 * MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("prefault"),
    )?;

    for p in [9usize, 0, 5] {
        let _ = region.read_byte(p * PAGE);
    }
    assert_eq!(handler.faulted_pages(), vec![0, 5, 9]);

    // Page 5 is present already, page 4096 lies past the region.
    handler.prefault(vec![1, 2, 3, 5, 4096])?;
    assert!(
        wait_for(|| handler.stats().prefaulted == 3, Duration::from_secs(5)),
        "three pages were prefaulted: {:?}",
        handler.stats()
    );
    let faults_before = handler.stats().faults;
    for p in [1usize, 2, 3] {
        let got = region.read(p * PAGE, PAGE);
        assert_eq!(
            &got[..],
            &source.as_slice()[p * PAGE..(p + 1) * PAGE],
            "page {p}"
        );
    }
    assert_eq!(
        handler.stats().faults,
        faults_before,
        "prefaulted pages do not fault"
    );
    assert_eq!(handler.faulted_pages(), vec![0, 1, 2, 3, 5, 9]);
    handler.stop()?;
    Ok(())
}

#[test]
fn stop_returns_with_more_hung_faults_than_install_slots() -> Result<()> {
    let Some(uffd) = uffd_or_skip("stop_returns_with_more_hung_faults_than_install_slots") else {
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
        HandlerOptions {
            max_inflight: 2,
            ..opts("hang-many")
        },
    )?;

    let faulters: Vec<_> = (1..=4)
        .map(|p| {
            let region = Arc::clone(&region);
            std::thread::spawn(move || region.read_byte(p * PAGE))
        })
        .collect();
    // Every fault is read off the descriptor although only two can be in
    // flight; the rest queue behind the slots.
    assert!(
        wait_for(|| handler.stats().faults >= 4, Duration::from_secs(5)),
        "the event loop kept reading while every slot was taken: {:?}",
        handler.stats()
    );
    assert!(faulters.iter().all(|f| !f.is_finished()));
    let started = Instant::now();
    handler.stop()?;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stop returned promptly"
    );
    for faulter in faulters {
        assert_eq!(faulter.join().expect("faulting thread"), 0);
    }
    Ok(())
}

/// A source whose reads wait for the test to release them, then fill the
/// page with a pattern.
struct GatedSource {
    size: u64,
    entered: AtomicBool,
    gate: tokio::sync::Notify,
}

impl PageSource for GatedSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_page<'a>(
        &'a self,
        _ring: &'a AsyncIoRing,
        _offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::SeqCst);
            self.gate.notified().await;
            dst.fill(0xAB);
            Ok(())
        })
    }
}

#[test]
fn a_remove_during_an_inflight_read_installs_a_zero_page() -> Result<()> {
    let Some(uffd) = uffd_or_skip("a_remove_during_an_inflight_read_installs_a_zero_page") else {
        return Ok(());
    };
    let size = MIB;
    let region = Arc::new(AnonRegion::new(size)?);
    let granted = region.register(&uffd, UFFD_FEATURE_EVENT_REMOVE)?;
    if granted & UFFD_FEATURE_EVENT_REMOVE == 0 {
        eprintln!(
            "SKIPPED[uffd]: a_remove_during_an_inflight_read_installs_a_zero_page (no EVENT_REMOVE)"
        );
        return Ok(());
    }
    let source = Arc::new(GatedSource {
        size: size as u64,
        entered: AtomicBool::new(false),
        gate: tokio::sync::Notify::new(),
    });
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, PAGE as u64)],
        Arc::clone(&source),
        opts("remove-race"),
    )?;

    let p = 7;
    let faulter = {
        let region = Arc::clone(&region);
        std::thread::spawn(move || region.read_byte(p * PAGE))
    };
    assert!(
        wait_for(
            || source.entered.load(Ordering::SeqCst),
            Duration::from_secs(5)
        ),
        "the read started"
    );
    // The discard returns once the handler has read the REMOVE event.
    region.discard(p * PAGE, PAGE)?;
    assert!(
        wait_for(|| handler.stats().removes >= 1, Duration::from_secs(5)),
        "the REMOVE event reached the handler"
    );
    source.gate.notify_one();
    let byte = faulter.join().expect("faulting thread");
    assert_eq!(
        byte, 0,
        "the read that finished after the REMOVE did not land"
    );
    let stats = handler.stats();
    assert_eq!(stats.pages_copied, 0);
    assert_eq!(stats.pages_zeroed, 1);
    handler.stop()?;
    Ok(())
}

#[test]
fn a_fault_outside_every_region_gets_a_zero_page_and_serving_goes_on() -> Result<()> {
    let Some(uffd) =
        uffd_or_skip("a_fault_outside_every_region_gets_a_zero_page_and_serving_goes_on")
    else {
        return Ok(());
    };
    let size = 2 * MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(MIB, PAGE, 0));
    // The handshake covers the first half only; the kernel has the whole
    // region registered.
    let mut mapping = region.mapping(0, PAGE as u64);
    mapping.size = MIB as u64;
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![mapping],
        Arc::clone(&source),
        opts("unmapped"),
    )?;

    let got = region.read(MIB + 5 * PAGE, PAGE);
    assert!(got.iter().all(|b| *b == 0));
    assert!(
        wait_for(|| handler.stats().unmapped == 1, Duration::from_secs(5)),
        "{:?}",
        handler.stats()
    );
    assert!(handler.is_running());
    assert_eq!(
        region.read_byte(3 * PAGE + 2),
        source.as_slice()[3 * PAGE + 2]
    );
    handler.stop()?;
    Ok(())
}

#[test]
fn a_read_that_hangs_is_retried_and_ends_the_handler_after_its_budget() -> Result<()> {
    let Some(uffd) =
        uffd_or_skip("a_read_that_hangs_is_retried_and_ends_the_handler_after_its_budget")
    else {
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
        HandlerOptions {
            read_timeout: Duration::from_millis(100),
            read_retry_budget: Duration::from_millis(600),
            ..opts("hang-timeout")
        },
    )?;
    let faulter = {
        let region = Arc::clone(&region);
        std::thread::spawn(move || region.read_byte(2 * PAGE))
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let err = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), handler.wait_exit()).await
    })?;
    let err = err.unwrap_or_default();
    assert!(err.contains("timed out"), "{err}");
    assert!(handler.stats().read_retries >= 1);
    assert_eq!(faulter.join().expect("faulting thread"), 0);
    assert!(handler.stop().is_err());
    Ok(())
}

#[test]
fn a_handshake_body_split_across_segments_is_reassembled() -> Result<()> {
    let Some(uffd) = uffd_or_skip("a_handshake_body_split_across_segments_is_reassembled") else {
        return Ok(());
    };
    use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags, UnixAddr};
    use std::io::{IoSlice, Write};
    use std::os::fd::AsRawFd;

    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("uffd.sock");
    let listener = UnixListener::bind(&socket)?;
    let size = MIB;
    let region = AnonRegion::new(size)?;
    region.register(&uffd, 0)?;
    let source = Arc::new(MemSource::patterned(size, PAGE, 0));
    let handler = UffdHandler::serve_socket(listener, Arc::clone(&source), opts("split"))?;

    let mut stream = UnixStream::connect(&socket)?;
    let body = serde_json::to_vec(&vec![region.mapping(0, PAGE as u64)])?;
    let half = body.len() / 2;
    let fds = [uffd.as_raw_fd()];
    let cmsg = [ControlMessage::ScmRights(&fds)];
    let sent = sendmsg::<UnixAddr>(
        stream.as_raw_fd(),
        &[IoSlice::new(&body[..half])],
        &cmsg,
        MsgFlags::empty(),
        None,
    )?;
    assert_eq!(sent, half);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(handler.state(), HandlerState::Starting);
    stream.write_all(&body[half..])?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), handler.wait_serving()).await
    })??;
    drop(uffd);
    assert_eq!(
        region.read_byte(9 * PAGE + 1),
        source.as_slice()[9 * PAGE + 1]
    );
    handler.stop()?;
    Ok(())
}

#[test]
fn hugepage_regions_are_served_a_whole_page_at_a_time() -> Result<()> {
    let Some(uffd) = uffd_or_skip("hugepage_regions_are_served_a_whole_page_at_a_time") else {
        return Ok(());
    };
    const HUGE: usize = 2 << 20;
    let size = 4 * HUGE;
    let region = match AnonRegion::new_hugetlb(size) {
        Ok(region) => region,
        Err(err) => {
            eprintln!(
                "SKIPPED[uffd]: hugepage_regions_are_served_a_whole_page_at_a_time (no hugetlb pool: {err:#})"
            );
            return Ok(());
        }
    };
    let granted = region.register(&uffd, UFFD_FEATURE_MISSING_HUGETLBFS)?;
    if granted & UFFD_FEATURE_MISSING_HUGETLBFS == 0 {
        eprintln!("SKIPPED[uffd]: hugepage_regions_are_served_a_whole_page_at_a_time (no MISSING_HUGETLBFS)");
        return Ok(());
    }
    // Every other 2 MiB page is zero, so both install paths run on a page
    // size that has no shared zero page.
    let source = Arc::new(MemSource::patterned(size, HUGE, 2));
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![region.mapping(0, HUGE as u64)],
        Arc::clone(&source),
        opts("huge"),
    )?;

    for p in 0..4 {
        let at = p * HUGE + 1234;
        assert_eq!(
            region.read(at, 64),
            &source.as_slice()[at..at + 64],
            "page {p}"
        );
    }
    assert_eq!(handler.page_size(), Some(HUGE as u64));
    let stats = handler.stats();
    assert_eq!(stats.pages_copied + stats.pages_zeroed, 4, "{stats:?}");
    assert_eq!(stats.pages_zeroed, 2, "{stats:?}");
    assert_eq!(stats.bytes_read, size as u64);
    // A second touch anywhere in a served page does not fault.
    let before = handler.stats().faults;
    let _ = region.read_byte(3 * HUGE + HUGE - 1);
    assert_eq!(handler.stats().faults, before);
    handler.stop()?;
    Ok(())
}

#[test]
fn write_protected_pages_read_clean_and_a_write_shows_up_as_dirty() -> Result<()> {
    let Some(uffd) = uffd_or_skip("write_protected_pages_read_clean_and_a_write_shows_up_as_dirty")
    else {
        return Ok(());
    };
    let size = MIB;
    let region = Arc::new(AnonRegion::new(size)?);
    let granted = match region.register_with_mode(
        &uffd,
        UFFD_FEATURE_PAGEFAULT_FLAG_WP,
        UFFDIO_REGISTER_MODE_MISSING | UFFDIO_REGISTER_MODE_WP,
    ) {
        Ok(granted) => granted,
        Err(err) => {
            eprintln!(
                "SKIPPED[uffd]: write_protected_pages_read_clean_and_a_write_shows_up_as_dirty (no write protection: {err:#})"
            );
            return Ok(());
        }
    };
    if granted & UFFD_FEATURE_PAGEFAULT_FLAG_WP == 0 {
        eprintln!("SKIPPED[uffd]: write_protected_pages_read_clean_and_a_write_shows_up_as_dirty (no PAGEFAULT_FLAG_WP)");
        return Ok(());
    }
    // Every fourth page is zero, so the zero path is exercised under
    // protection too.
    let source = Arc::new(MemSource::patterned(size, PAGE, 4));
    let mapping = region.mapping(0, PAGE as u64);
    let handler = UffdHandler::serve_fd(
        uffd.into_owned_fd(),
        vec![mapping.clone()],
        Arc::clone(&source),
        opts("wp"),
    )?;
    let pid = std::process::id();

    assert_eq!(region.read_byte(PAGE + 5), source.as_slice()[PAGE + 5]);
    assert!(region.read(4 * PAGE, PAGE).iter().all(|b| *b == 0));
    assert_eq!(handler.write_protect(), Some(true));
    let dirty = dirty_ranges(
        pid,
        std::slice::from_ref(&mapping),
        DirtySource::UffdWriteProtect,
    )?;
    assert!(dirty.is_empty(), "read pages are clean: {dirty:?}");

    // The first write is a write-protect fault the handler resolves by
    // unprotecting the page; the cleared bit is the dirty mark.
    region.write_byte(PAGE + 7, 0xAA);
    assert_eq!(region.read_byte(PAGE + 7), 0xAA);
    region.write_byte(4 * PAGE + 1, 0xBB);
    let stats = handler.stats();
    assert_eq!(stats.wp_faults, 2, "{stats:?}");
    assert_eq!(stats.pages_copied + stats.pages_zeroed, 2);
    let dirty = dirty_ranges(
        pid,
        std::slice::from_ref(&mapping),
        DirtySource::UffdWriteProtect,
    )?;
    assert_eq!(
        dirty,
        vec![
            DirtyRange {
                host_addr: region.addr() + PAGE as u64,
                image_offset: PAGE as u64,
                len: PAGE as u64,
            },
            DirtyRange {
                host_addr: region.addr() + 4 * PAGE as u64,
                image_offset: 4 * PAGE as u64,
                len: PAGE as u64,
            },
        ]
    );
    // A second write to an unprotected page is not a fault.
    region.write_byte(PAGE + 8, 0xCC);
    assert_eq!(handler.stats().wp_faults, 2);
    handler.stop()?;
    Ok(())
}
