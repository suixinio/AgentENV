mod support;

use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::*;
use uvm_nbd::{MemTarget, NbdDevice, NbdOptions};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

fn timeout_options(connections: u16) -> NbdOptions {
    NbdOptions {
        connections,
        io_timeout: IO_TIMEOUT,
        queue_depth: 8,
        ..options(connections)
    }
}

struct ReadOutcome {
    elapsed: Duration,
    result: std::io::Result<Vec<u8>>,
}

/// A pread on its own thread, so the test can watch a request the device is
/// not answering.
fn spawn_read(path: &std::path::Path, offset: u64, len: usize) -> mpsc::Receiver<ReadOutcome> {
    let path = path.to_path_buf();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let started = Instant::now();
        let result = (|| {
            let file = direct(&path, false)?;
            let mut buf = Aligned::new(len);
            std::os::unix::fs::FileExt::read_exact_at(&file, buf.as_mut(), offset)?;
            Ok(buf.as_ref().to_vec())
        })();
        let _ = tx.send(ReadOutcome {
            elapsed: started.elapsed(),
            result,
        });
    });
    rx
}

#[tokio::test]
async fn a_stalled_target_fails_the_reader_within_the_io_timeout() {
    let name = "a_stalled_target_fails_the_reader_within_the_io_timeout";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    target.fill(0, &pattern(64 * 1024, 0x3C));
    let device = NbdDevice::start(target.clone(), timeout_options(1))
        .await
        .expect("start the nbd device");
    let index = device.index();

    target.stall(true);
    let outcome = spawn_read(device.device_path(), 0, 4096)
        .recv_timeout(IO_TIMEOUT * 8)
        .expect("the reader must not hang past the io timeout");
    target.stall(false);

    let errno = outcome
        .result
        .as_ref()
        .err()
        .and_then(|err| err.raw_os_error());
    eprintln!(
        "nbd{index}: one connection, a stalled read failed with errno {errno:?} after {:?}",
        outcome.elapsed
    );
    assert_eq!(
        errno,
        Some(libc::EIO),
        "a request the server never answers must reach the reader as EIO"
    );
    assert!(
        outcome.elapsed >= IO_TIMEOUT && outcome.elapsed < IO_TIMEOUT * 4,
        "the reader waited {:?}, which the {IO_TIMEOUT:?} io timeout does not explain",
        outcome.elapsed
    );

    // The timeout does not merely fail one request: the kernel takes the
    // connection down with it, and a device whose last connection is gone
    // answers everything with EIO until it is reattached.
    let after = spawn_read(device.device_path(), 0, 4096)
        .recv_timeout(Duration::from_secs(30))
        .expect("a read on a timed-out device must not hang either");
    eprintln!(
        "nbd{index}: after un-stalling, a fresh read still failed with {:?} after {:?}",
        after
            .result
            .as_ref()
            .err()
            .and_then(|err| err.raw_os_error()),
        after.elapsed
    );
    assert!(
        after.result.is_err(),
        "un-stalling the target does not revive a device whose connection the timeout killed"
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_target_slower_than_a_request_but_inside_the_timeout_still_answers() {
    let name = "a_target_slower_than_a_request_but_inside_the_timeout_still_answers";
    if !nbd_available(name) {
        return;
    }
    let payload = pattern(4096, 0x5B);
    let target = Arc::new(MemTarget::new(16 * MIB, BLOCK_SIZE));
    target.fill(0, &payload);
    let device = NbdDevice::start(target.clone(), timeout_options(1))
        .await
        .expect("start the nbd device");
    let index = device.index();

    target.stall(true);
    let reader = spawn_read(device.device_path(), 0, 4096);
    std::thread::sleep(IO_TIMEOUT / 3);
    target.stall(false);

    let outcome = reader
        .recv_timeout(Duration::from_secs(30))
        .expect("a target that answers inside the budget must not hang the reader");
    eprintln!(
        "nbd{index}: a {:?} stall inside a {IO_TIMEOUT:?} budget returned after {:?}",
        IO_TIMEOUT / 3,
        outcome.elapsed
    );
    assert_eq!(
        outcome
            .result
            .expect("a slow but answered read must succeed"),
        payload,
        "a request slower than usual must be served, not failed"
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_stalled_target_costs_one_io_timeout_per_connection_before_it_fails() {
    let name = "a_stalled_target_costs_one_io_timeout_per_connection_before_it_fails";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(target.clone(), timeout_options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    target.stall(true);
    let outcome = spawn_read(device.device_path(), 0, 4096)
        .recv_timeout(IO_TIMEOUT * 12)
        .expect("the reader must not hang past the io timeout");
    target.stall(false);
    let errno = outcome
        .result
        .as_ref()
        .err()
        .and_then(|err| err.raw_os_error());
    eprintln!(
        "nbd{index}: two connections, a stalled read failed with errno {errno:?} after {:?}",
        outcome.elapsed
    );
    assert_eq!(errno, Some(libc::EIO));
    assert!(
        outcome.elapsed >= IO_TIMEOUT,
        "the reader gave up after {:?}, before even one io timeout could have fired",
        outcome.elapsed
    );
    // Whether the kernel retries onto the second connection before giving up
    // is not fixed -- observed between one and three timeouts -- but one
    // timeout per connection plus a margin bounds it either way.
    assert!(
        outcome.elapsed < IO_TIMEOUT * 4,
        "the reader waited {:?}, which one io timeout per connection does not explain",
        outcome.elapsed
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_request_with_no_live_connection_fails_after_the_dead_connection_timeout() {
    let name = "a_request_with_no_live_connection_fails_after_the_dead_connection_timeout";
    if !nbd_available(name) {
        return;
    }
    let dead_conn = Duration::from_secs(2);
    let target = Arc::new(MemTarget::new(16 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(
        target,
        NbdOptions {
            io_timeout: Duration::from_secs(30),
            dead_conn_timeout: Some(dead_conn),
            ..options(1)
        },
    )
    .await
    .expect("start the nbd device");
    let path = device.device_path().to_path_buf();
    let index = device.abandon().await;

    assert!(
        matches!(sys_block_size(index), Some(size) if size > 0),
        "an abandoned device must stay configured; that is what makes a reattach possible"
    );

    let outcome = spawn_read(&path, 0, 4096)
        .recv_timeout(Duration::from_secs(60))
        .expect("a read with no live connection must not hang");
    let errno = outcome
        .result
        .as_ref()
        .err()
        .and_then(|err| err.raw_os_error());
    eprintln!(
        "nbd{index}: with every connection dead, a read failed with errno {errno:?} after {:?} \
         (dead_conn_timeout was {dead_conn:?}, io_timeout 30s)",
        outcome.elapsed
    );
    assert!(
        outcome.result.is_err(),
        "no server can answer, so the read must fail rather than return bytes"
    );
    assert!(
        outcome.elapsed < Duration::from_secs(15),
        "the read waited {:?}: the dead-connection timeout, not the io timeout, is what bounds it",
        outcome.elapsed
    );

    // The kernel shuts the device down when the reconnect window expires; clean
    // up whatever is left either way.
    if let Ok(netlink) = uvm_nbd::NbdNetlink::open() {
        let _ = netlink.disconnect(index);
    }
}

#[tokio::test]
async fn a_reattached_device_serves_reads_again_from_a_fresh_target() {
    let name = "a_reattached_device_serves_reads_again_from_a_fresh_target";
    if !nbd_available(name) {
        return;
    }
    let payload = pattern(4096, 0x6E);
    let first = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    first.fill(8192, &payload);

    // A reconnect window wide enough for the reattach: without it a request
    // that finds every socket dead is answered EIO at once, and the kernel
    // shuts the device down on the way out.
    let options = NbdOptions {
        dead_conn_timeout: Some(Duration::from_secs(20)),
        ..timeout_options(2)
    };
    let device = NbdDevice::start(first, options.clone())
        .await
        .expect("start the nbd device");
    let index = device.index();
    let path = device.device_path().to_path_buf();

    // The server dies without a DISCONNECT: the sockets close under the kernel
    // and its connection slots go dead.
    assert_eq!(device.abandon().await, index);
    assert!(
        matches!(sys_block_size(index), Some(size) if size > 0),
        "an abandoned device must stay configured; that is what makes a reattach possible"
    );

    let second = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    second.fill(8192, &payload);
    let reattached = reattach_within(index, &second, &options).await;
    assert_eq!(reattached.index(), index);

    // A request issued once the new connections are in place is served by them
    // like any other.
    for round in 0..3 {
        let outcome = spawn_read(&path, 8192, 4096)
            .recv_timeout(Duration::from_secs(60))
            .expect("a read issued after the reattach must complete");
        assert_eq!(
            outcome
                .result
                .unwrap_or_else(|err| panic!("read {round} after the reattach failed: {err:?}")),
            payload,
            "the reattached server served the wrong bytes"
        );
    }

    let mut written = Aligned::filled(4096, 0x2A);
    {
        let file = direct(&path, true).expect("open the reattached device for write");
        std::os::unix::fs::FileExt::write_all_at(&file, written.as_ref(), 16384)
            .expect("write through the reattached device");
        file.sync_data().expect("fdatasync");
    }
    assert_eq!(
        second.snapshot(16384, 4096),
        written.as_mut(),
        "writes after the reattach must land in the new target"
    );

    reattached.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_read_in_flight_when_the_server_crashes_is_re_sent_to_its_replacement() {
    let name = "a_read_in_flight_when_the_server_crashes_is_re_sent_to_its_replacement";
    if !nbd_available(name) {
        return;
    }
    let reconnect_window = Duration::from_secs(10);
    let payload = pattern(4096, 0x71);
    let first = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    first.fill(8192, &payload);
    let options = NbdOptions {
        dead_conn_timeout: Some(reconnect_window),
        ..timeout_options(2)
    };
    let device = NbdDevice::start(first.clone(), options.clone())
        .await
        .expect("start the nbd device");
    let index = device.index();
    let path = device.device_path().to_path_buf();

    first.stall(true);
    let reader = spawn_read(&path, 8192, 4096);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(device.abandon().await, index);

    let second = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    second.fill(8192, &payload);
    let reattached = reattach_within(index, &second, &options).await;

    let outcome = reader
        .recv_timeout(reconnect_window * 4)
        .expect("a request in flight at the crash must not leave the reader hanging");
    eprintln!(
        "nbd{index}: the in-flight read was re-sent and answered after {:?}",
        outcome.elapsed
    );
    assert_eq!(
        outcome
            .result
            .expect("the request the dead server never answered must be re-sent, not failed"),
        payload,
        "the reattached server served the wrong bytes"
    );
    // One io timeout is what the re-send costs: the kernel notices the request
    // is unanswered on its own schedule, not when the socket dies.
    assert!(
        outcome.elapsed >= IO_TIMEOUT,
        "the read finished in {:?}, before the request could have been re-sent",
        outcome.elapsed
    );

    reattached.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

/// The kernel fills a connection slot only once it has noticed the old socket
/// die, so a reattach right after a crash is retried until it lands.
async fn reattach_within(index: u32, target: &Arc<MemTarget>, options: &NbdOptions) -> NbdDevice {
    let started = Instant::now();
    for attempt in 0..100 {
        match NbdDevice::reattach(index, target.clone(), options.clone()).await {
            Ok(device) => {
                eprintln!(
                    "nbd{index}: reattach succeeded on attempt {attempt} after {:?}",
                    started.elapsed()
                );
                return device;
            }
            Err(err) if attempt == 99 => panic!("reattach never succeeded: {err:#}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    unreachable!("the loop either returns or panics")
}
