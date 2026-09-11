//! Integration tests for the `uvm-ublk-daemon` crate.
//!
//! These tests exercise the client-server RPC protocol using a lightweight
//! mock server that speaks the same length-prefixed JSON wire format. This
//! lets us thoroughly test client-side RPC logic, response mapping, and
//! error handling without requiring ublk hardware or kernel modules.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use overlaybd::config::UpperMode;
use tokio::net::UnixListener;
use tokio::sync::oneshot;

use uvm_ublk_daemon::protocol::{
    recv_message, send_message, DaemonRequest, DaemonResponse, MemoryUffdState, MemoryUffdStats,
};
use uvm_ublk_daemon::{
    CreateOverlaybdRuntimeDeviceRequest, InvalidRequestError, RestackSnapshotTerminalFailure,
    Transport, TransportHandle, UblkDaemonClient,
};

// ════════════════════════════════════════════════════════════════════════════
// Transport selection
// ════════════════════════════════════════════════════════════════════════════

const TRANSPORT_ENV: &str = "AENV_DAEMON_TEST_TRANSPORT";

/// The transport the whole suite runs against. `ublk` unless
/// `AENV_DAEMON_TEST_TRANSPORT=nbd`.
fn selected_transport() -> Transport {
    match std::env::var(TRANSPORT_ENV) {
        Ok(value) if !value.trim().is_empty() => value
            .parse()
            .unwrap_or_else(|err| panic!("{TRANSPORT_ENV}: {err}")),
        _ => Transport::Ublk,
    }
}

/// Keeps the ublk control ring's worker thread alive for as long as the server
/// that borrows it.
struct TestTransport {
    handle: TransportHandle,
    _ctrl_ring_worker: Option<std::thread::JoinHandle<()>>,
}

impl TestTransport {
    fn handle(&self) -> TransportHandle {
        self.handle.clone()
    }
}

fn test_transport() -> TestTransport {
    match selected_transport() {
        Transport::Ublk => {
            let (ctrl_ring, worker) =
                storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
            TestTransport {
                handle: TransportHandle::Ublk(ctrl_ring),
                _ctrl_ring_worker: Some(worker),
            }
        }
        Transport::Nbd => TestTransport {
            handle: TransportHandle::Nbd(uvm_nbd::NbdOptions {
                connections: 2,
                ..Default::default()
            }),
            _ctrl_ring_worker: None,
        },
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Mock server infrastructure
// ════════════════════════════════════════════════════════════════════════════

/// A handler function that takes a request and returns a response.
type MockHandler = Box<dyn Fn(DaemonRequest) -> DaemonResponse + Send + Sync>;

/// A lightweight mock server that listens on a Unix socket and dispatches
/// incoming requests to a user-provided handler function.
struct MockServer {
    socket_path: PathBuf,
    _dir: tempfile::TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockServer {
    /// Start a mock server with the given handler. Returns immediately;
    /// the server runs in a background task.
    async fn start(handler: MockHandler) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((mut stream, _)) => {
                                let handler = Arc::clone(&handler);
                                tokio::spawn(async move {
                                    if let Ok(Some(request)) =
                                        recv_message::<DaemonRequest>(&mut stream).await
                                    {
                                        let response = handler(request);
                                        let _ = send_message(&mut stream, &response).await;
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a mock server that accepts a connection but never responds.
    async fn start_hanging() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((_stream, _)) => {
                                // Hold the stream open but never respond.
                                // Keep it alive until shutdown.
                                tokio::time::sleep(Duration::from_secs(300)).await;
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a mock server that accepts and immediately closes the connection.
    async fn start_drop_connection() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((_stream, _)) => {
                                // Drop the stream immediately → EOF for client.
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Create a `UblkDaemonClient` pointing at this mock server.
    fn client(&self) -> Arc<UblkDaemonClient> {
        UblkDaemonClient::new_for_test(self.socket_path.clone(), false)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Protocol integration tests
// ════════════════════════════════════════════════════════════════════════════

mod protocol_tests {
    use super::*;

    /// Test bidirectional exchange: client sends request, server sends response.
    #[tokio::test]
    async fn bidirectional_request_response() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("bidir.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::UnixStream::connect(&sock_path_clone)
                .await
                .unwrap();
            let req = DaemonRequest::Delete { dev_id: 42 };
            send_message(&mut stream, &req).await.unwrap();

            let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
            resp
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let req: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        assert!(matches!(req, DaemonRequest::Delete { dev_id: 42 }));

        let resp = DaemonResponse::Deleted;
        send_message(&mut stream, &resp).await.unwrap();

        let client_resp = client_task.await.unwrap();
        assert!(matches!(client_resp, DaemonResponse::Deleted));
    }

    /// Test sending a request with a long path (exercising larger payloads).
    #[tokio::test]
    async fn large_payload_path() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("large.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        // Create a path string with 100K characters.
        let long_path = "/".to_string() + &"a".repeat(100_000);

        let sock_path_clone = sock_path.clone();
        let long_path_clone = long_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = tokio::net::UnixStream::connect(&sock_path_clone)
                .await
                .unwrap();
            let req = DaemonRequest::CreateOverlaybd {
                image_config: PathBuf::from(&long_path_clone),
                global_config: PathBuf::from("/etc/overlaybd/global.json"),
            };
            send_message(&mut stream, &req).await.unwrap();
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let msg: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        sender.await.unwrap();

        match msg {
            DaemonRequest::CreateOverlaybd { image_config, .. } => {
                assert_eq!(image_config.to_str().unwrap().len(), long_path.len());
            }
            _ => panic!("unexpected variant"),
        }
    }

    /// Test concurrent clients sending on separate connections.
    #[tokio::test]
    async fn concurrent_clients() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("concurrent.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let num_clients = 10;

        // Spawn N clients that each send a Delete request.
        let mut client_handles = Vec::new();
        for i in 0..num_clients {
            let path = sock_path.clone();
            client_handles.push(tokio::spawn(async move {
                let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
                let req = DaemonRequest::Delete { dev_id: i };
                send_message(&mut stream, &req).await.unwrap();
            }));
        }

        // Accept all N connections and verify.
        let mut received_ids = Vec::new();
        for _ in 0..num_clients {
            let (mut stream, _) = listener.accept().await.unwrap();
            let msg: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
            match msg {
                DaemonRequest::Delete { dev_id } => received_ids.push(dev_id),
                _ => panic!("unexpected variant"),
            }
        }

        for handle in client_handles {
            handle.await.unwrap();
        }

        received_ids.sort();
        let expected: Vec<u32> = (0..num_clients).collect();
        assert_eq!(received_ids, expected);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Client RPC tests against mock server
// ════════════════════════════════════════════════════════════════════════════

mod client_tests {
    use super::*;

    // ── create_overlaybd ────────────────────────────────────────────────

    #[tokio::test]
    async fn create_overlaybd_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybd { .. } => DaemonResponse::DeviceCreated {
                dev_id: 5,
                device_path: PathBuf::from("/dev/ublkb5"),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .create_overlaybd(Path::new("/tmp/image.json"), Path::new("/global.json"))
            .await
            .unwrap();
        assert_eq!(dev_id, 5);
        assert_eq!(path, PathBuf::from("/dev/ublkb5"));
    }

    #[tokio::test]
    async fn create_overlaybd_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "disk full".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("disk full"),
            "error should contain message from daemon: {msg}"
        );
    }

    #[tokio::test]
    async fn create_overlaybd_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unexpected"),
            "error should mention unexpected response: {msg}"
        );
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybdRuntimeDevice {
                source_image_config,
                global_config,
                runtime_dir,
                read_only,
                runtime_upper_mode,
                requested_virtual_size,
                known_source_virtual_size,
                allow_shrink,
            } => {
                assert_eq!(source_image_config, PathBuf::from("/src/image.json"));
                assert_eq!(global_config, PathBuf::from("/global.json"));
                assert_eq!(runtime_dir, PathBuf::from("/work/overlaybd"));
                assert!(!read_only);
                assert_eq!(runtime_upper_mode, UpperMode::Sparse);
                assert_eq!(requested_virtual_size, None);
                assert_eq!(known_source_virtual_size, Some(8192));
                assert!(!allow_shrink);
                DaemonResponse::OverlaybdRuntimeDeviceCreated {
                    dev_id: 11,
                    device_path: PathBuf::from("/dev/ublkb11"),
                    actual_virtual_size: 8192,
                    runtime_image_config_path: PathBuf::from("/work/overlaybd/image.json"),
                }
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let device = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::Sparse,
                requested_virtual_size: None,
                known_source_virtual_size: Some(8192),
                allow_shrink: false,
            })
            .await
            .unwrap();
        assert_eq!(device.dev_id, 11);
        assert_eq!(device.device_path, PathBuf::from("/dev/ublkb11"));
        assert_eq!(device.actual_virtual_size, 8192);
        assert_eq!(
            device.runtime_image_config_path,
            PathBuf::from("/work/overlaybd/image.json")
        );
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_error() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybdRuntimeDevice { .. } => DaemonResponse::Error {
                message: "bad runtime".into(),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let err = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("bad runtime"));
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_preserves_invalid_request_type() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::InvalidRequest {
            message: "disk size is invalid".into(),
        }))
        .await;
        let err = server
            .client()
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<InvalidRequestError>().is_some());
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("unexpected response"));
    }

    // ── delete ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
            _ => DaemonResponse::Error {
                message: "unexpected".into(),
            },
        }))
        .await;

        let client = server.client();
        client.delete(7).await.unwrap();
    }

    #[tokio::test]
    async fn delete_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "device 99 not found".into(),
        }))
        .await;

        let client = server.client();
        let err = client.delete(99).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("not found"), "error: {msg}");
    }

    #[tokio::test]
    async fn delete_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::DeviceCreated {
            dev_id: 0,
            device_path: PathBuf::from("/dev/ublkb0"),
        }))
        .await;

        let client = server.client();
        let err = client.delete(1).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unexpected"), "error: {msg}");
    }

    // ── snapshot ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::RestackSnapshot { .. } => DaemonResponse::RestackSnapshotCreated {
                descriptor: Some(overlaybd::LayerDescriptor {
                    digest: "sha256:abc".to_string(),
                    size: 4096,
                }),
                data_stat: None,
                ext4_used_bytes: None,
            },
            _ => DaemonResponse::Error {
                message: "unexpected".into(),
            },
        }))
        .await;

        let client = server.client();
        let stats = client
            .restack_snapshot(2, Path::new("/snapshots/layer0"))
            .await
            .unwrap();
        assert_eq!(
            stats.descriptor,
            Some(overlaybd::LayerDescriptor {
                digest: "sha256:abc".to_string(),
                size: 4096,
            })
        );
    }

    #[tokio::test]
    async fn snapshot_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "snapshot failed".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .restack_snapshot(2, Path::new("/out"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("snapshot failed"), "error: {msg}");
    }

    #[tokio::test]
    async fn snapshot_terminal_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::TerminalError {
            message: "sealed upper left live runtime mutated".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .restack_snapshot(2, Path::new("/out"))
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<RestackSnapshotTerminalFailure>()
                .is_some(),
            "expected terminal restack failure, got: {err:#}"
        );
    }

    #[tokio::test]
    async fn snapshot_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .restack_snapshot(1, Path::new("/out"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unexpected"), "error: {msg}");
    }

    // ── shutdown ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn shutdown_succeeds_even_without_response() {
        let server = MockServer::start_drop_connection().await;
        let client = server.client();

        // shutdown() is best-effort — should succeed even if daemon doesn't respond.
        client.shutdown().await.unwrap();
    }

    // ── connection error cases ──────────────────────────────────────────

    #[tokio::test]
    async fn connection_refused_no_server() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("nonexistent.sock");
        let client = UblkDaemonClient::new_for_test(sock_path, false);

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("connect") || msg.contains("No such file"),
            "should fail with connection error: {msg}"
        );
    }

    #[tokio::test]
    async fn server_drops_connection_without_response() {
        let server = MockServer::start_drop_connection().await;
        let client = server.client();

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        // The client should get an error about missing response or EOF.
        assert!(
            msg.contains("closed") || msg.contains("EOF") || msg.contains("response"),
            "should fail with connection closed error: {msg}"
        );
    }

    #[tokio::test]
    async fn rpc_timeout() {
        let server = MockServer::start_hanging().await;

        // Create client with a very short timeout by calling the private call method
        // indirectly. Since we can't change the timeout on public methods, we test
        // that the server hanging eventually hits the 30s default timeout.
        // For practical test speed, we just verify the hanging server doesn't
        // cause an immediate success.
        let client = server.client();

        // Use a tokio timeout shorter than the default 30s to verify the client
        // is actually blocked waiting.
        let result =
            tokio::time::timeout(Duration::from_millis(500), async { client.delete(0).await })
                .await;

        assert!(
            result.is_err(),
            "should time out because mock server never responds"
        );
    }

    #[tokio::test]
    async fn daemon_dead_prevents_all_rpcs() {
        // Even with a valid server running, a dead daemon flag should prevent calls.
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        // Create client with daemon_dead=true.
        let client = UblkDaemonClient::new_for_test(server.socket_path.clone(), true);

        let err = client.delete(0).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("not running"), "error: {msg}");

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not running"));

        let err = client
            .restack_snapshot(0, Path::new("/out"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not running"));
    }

    // ── request dispatch verification ───────────────────────────────────

    #[tokio::test]
    async fn server_receives_correct_request_fields() {
        use std::sync::Mutex;

        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_for_server = Arc::clone(&captured);

        let server = MockServer::start(Box::new(move |req| {
            // Capture the request for later inspection.
            captured_for_server.lock().unwrap().push(format!("{req:?}"));
            match req {
                DaemonRequest::CreateOverlaybd { .. } => DaemonResponse::DeviceCreated {
                    dev_id: 10,
                    device_path: PathBuf::from("/dev/ublkb10"),
                },
                DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
                DaemonRequest::RestackSnapshot { .. } => DaemonResponse::RestackSnapshotCreated {
                    descriptor: None,
                    data_stat: None,
                    ext4_used_bytes: None,
                },
                DaemonRequest::Shutdown => DaemonResponse::Ok,
                DaemonRequest::GetFeatures => DaemonResponse::Features { flags: 0 },
                DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                    dev_id: 99,
                    device_path: PathBuf::from("/dev/ublkb99"),
                },
                DaemonRequest::CreateOverlaybdRuntimeDevice { .. } => {
                    DaemonResponse::OverlaybdRuntimeDeviceCreated {
                        dev_id: 100,
                        device_path: PathBuf::from("/dev/ublkb100"),
                        actual_virtual_size: 4096,
                        runtime_image_config_path: PathBuf::from("/work/overlaybd/image.json"),
                    }
                }
                DaemonRequest::ReleaseOverlaybd { .. } => DaemonResponse::Released,
                DaemonRequest::UpdateSize { .. } => DaemonResponse::SizeUpdated,
                DaemonRequest::NotifySandboxReady { .. } => DaemonResponse::Ok,
                DaemonRequest::ServeMemoryUffd { .. } => {
                    DaemonResponse::MemoryUffdServing { serve_id: 7 }
                }
                DaemonRequest::StopMemoryUffd { .. } => DaemonResponse::Ok,
                DaemonRequest::QueryMemoryUffd { .. } => DaemonResponse::MemoryUffdStatus {
                    state: MemoryUffdState::Serving,
                    stats: MemoryUffdStats::default(),
                    write_protect: None,
                    regions: Vec::new(),
                },
            }
        }))
        .await;

        let client = server.client();

        client
            .create_overlaybd(Path::new("/config/img.json"), Path::new("/global.json"))
            .await
            .unwrap();
        client.delete(30).await.unwrap();
        client
            .restack_snapshot(40, Path::new("/snap/output"))
            .await
            .unwrap();
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 3);

        assert!(requests[0].contains("CreateOverlaybd"));
        assert!(requests[0].contains("img.json"));
        assert!(requests[0].contains("global.json"));
        assert!(!requests[0].contains("dev_id"));

        assert!(requests[1].contains("Delete"));
        assert!(requests[1].contains("30"));

        assert!(requests[2].contains("RestackSnapshot"));
        assert!(requests[2].contains("40"));
        assert!(requests[2].contains("output"));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Server tests (accept loop, shutdown, stale socket)
// ════════════════════════════════════════════════════════════════════════════

mod server_tests {
    use super::*;
    use uvm_ublk_daemon::UblkDaemonServer;

    use overlaybd::image_service::ImageService;

    /// Helper to create a minimal `ImageService` for tests.
    ///
    /// We write a minimal global config JSON and construct from it. The
    /// image service won't be used for actual image operations in these
    /// tests — we only test the server's accept loop and shutdown.
    async fn test_image_service(dir: &Path) -> ImageService {
        let cache_dir = dir.join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config_path = dir.join("global_config.json");
        let config = serde_json::json!({
            "registryFsVersion": "v2",
            "ioEngine": 0,
            "cacheConfig": {
                "cacheType": "file",
                "cacheDir": cache_dir.to_str().unwrap(),
                "cacheSizeGB": 1,
                "refillSize": 262144,
                "blockSize": 65536
            }
        });
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
        ImageService::from_config_path(&config_path).await.unwrap()
    }

    #[tokio::test]
    async fn server_shutdown_via_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        );

        // Run server in background.
        let server_task = tokio::spawn(async move { server.run().await });

        // Wait a bit for the server to start listening.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a Shutdown request.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Shutdown)
            .await
            .unwrap();

        // Server should exit cleanly.
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok(), "server should exit without error");

        // Socket file should be cleaned up.
        assert!(
            !sock_path.exists(),
            "socket file should be removed after shutdown"
        );
    }

    #[tokio::test]
    async fn server_shutdown_via_notify() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        // Wait for the server to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Request shutdown via the notify mechanism.
        server.request_shutdown();

        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok(), "server should exit without error");
    }

    #[tokio::test]
    async fn server_cleans_up_stale_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        // Create a stale socket file (not actively listened on).
        let _stale = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
        drop(_stale);
        // The file exists but nobody is listening.
        assert!(sock_path.exists(), "stale socket should exist");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        // Wait for server to start (it should clean up the stale socket and rebind).
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify we can connect to the new server.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Shutdown)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_rejects_in_use_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        // Create a socket that IS actively being listened on.
        let _active_listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        );

        // Server should fail because the socket is in use.
        let result = server.run().await;
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("in use"), "expected 'in use' error: {msg}");
    }

    #[tokio::test]
    async fn server_handles_client_disconnect() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect and disconnect without sending anything.
        {
            let _stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
            // Drop immediately.
        }

        // Give server a moment to handle the disconnection.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Server should still be running.
        assert!(
            !server_task.is_finished(),
            "server should survive client disconnect"
        );

        // Shut down cleanly.
        server.request_shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("should stop")
            .expect("no panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_handles_invalid_request() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send garbage data.
        {
            let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
            let garbage = b"not json at all";
            let len = garbage.len() as u32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(garbage).await.unwrap();
            stream.flush().await.unwrap();
            // Give server time to process.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Server should still be running (error logged, connection closed).
        assert!(
            !server_task.is_finished(),
            "server should survive invalid request"
        );

        server.request_shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("should stop")
            .expect("no panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_handles_delete_nonexistent_device() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a Delete request for a device that doesn't exist.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Delete { dev_id: 999 })
            .await
            .unwrap();

        let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
        match resp {
            DaemonResponse::Error { message } => {
                assert!(
                    message.contains("not found"),
                    "expected 'not found' in error: {message}"
                );
            }
            other => panic!("expected Error response, got: {other:?}"),
        }

        // Clean up.
        server.request_shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
    }

    #[tokio::test]
    async fn server_handles_snapshot_nonexistent_device() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let transport = test_transport();
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            transport.handle(),
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(
            &mut stream,
            &DaemonRequest::RestackSnapshot {
                dev_id: 888,
                output_layer_path: PathBuf::from("/tmp/snap"),
            },
        )
        .await
        .unwrap();

        let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
        match resp {
            DaemonResponse::Error { message } => {
                assert!(
                    message.contains("not found"),
                    "expected 'not found' in error: {message}"
                );
            }
            other => panic!("expected Error response, got: {other:?}"),
        }

        server.request_shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
    }

    // ── Pool RPC tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_features_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::GetFeatures => DaemonResponse::Features { flags: 0x0001 },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let flags = client.get_features().await.unwrap();
        assert_eq!(flags, 0x0001);
    }

    #[tokio::test]
    async fn acquire_overlaybd_exclusive_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                dev_id: 10,
                device_path: PathBuf::from("/dev/ublkb10"),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .acquire_overlaybd(
                Path::new("/tmp/image.json"),
                Path::new("/global.json"),
                1024 * 1024 * 1024, // 1GB
                uvm_ublk_daemon::AccessMode::Exclusive,
            )
            .await
            .unwrap();
        assert_eq!(dev_id, 10);
        assert_eq!(path, PathBuf::from("/dev/ublkb10"));
    }

    #[tokio::test]
    async fn acquire_overlaybd_shared_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { access_mode, .. } => {
                assert_eq!(access_mode, uvm_ublk_daemon::AccessMode::Shared);
                DaemonResponse::DeviceAcquired {
                    dev_id: 20,
                    device_path: PathBuf::from("/dev/ublkb20"),
                }
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .acquire_overlaybd(
                Path::new("/tmp/mem.json"),
                Path::new("/global.json"),
                128 * 1024 * 1024, // 128MB
                uvm_ublk_daemon::AccessMode::Shared,
            )
            .await
            .unwrap();
        assert_eq!(dev_id, 20);
        assert_eq!(path, PathBuf::from("/dev/ublkb20"));
    }

    #[tokio::test]
    async fn release_overlaybd_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::ReleaseOverlaybd { dev_id } => {
                assert_eq!(dev_id, 15);
                DaemonResponse::Released
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        client.release_overlaybd(15).await.unwrap();
    }

    #[tokio::test]
    async fn update_size_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::UpdateSize {
                dev_id,
                new_sectors,
            } => {
                assert_eq!(dev_id, 25);
                assert_eq!(new_sectors, 2048);
                DaemonResponse::SizeUpdated
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        client.update_size(25, 2048).await.unwrap();
    }

    #[tokio::test]
    async fn acquire_overlaybd_error() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::Error {
                message: "pool not enabled".into(),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let result = client
            .acquire_overlaybd(
                Path::new("/tmp/image.json"),
                Path::new("/global.json"),
                1024 * 1024 * 1024,
                uvm_ublk_daemon::AccessMode::Exclusive,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("pool not enabled"));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Live device tests: a real server over the selected transport, real devices
// ════════════════════════════════════════════════════════════════════════════

mod live_device_tests {
    use super::*;
    use overlaybd::config::UpperMode as OverlaybdUpperMode;
    use overlaybd::image_service::ImageService;
    use std::os::unix::fs::FileExt;
    use uvm_ublk_daemon::{AccessMode, UblkDaemonServer};

    /// Live devices need the selected transport to be reachable from this
    /// process; `AENV_NBD_TEST_REQUIRED=1` turns a skip into a failure.
    fn transport_reachable(test: &str) -> bool {
        let reason = match selected_transport() {
            Transport::Nbd if uvm_ublk_daemon::nbd_transport_usable() => return true,
            Transport::Nbd => "the nbd transport is not reachable here",
            Transport::Ublk if Path::new("/dev/ublk-control").exists() => return true,
            Transport::Ublk => "/dev/ublk-control is absent; the ublk_drv module is not loaded",
        };
        if std::env::var("AENV_NBD_TEST_REQUIRED").as_deref() == Ok("1")
            && selected_transport() == Transport::Nbd
        {
            panic!("AENV_NBD_TEST_REQUIRED=1 but {test} cannot run: {reason}");
        }
        eprintln!("SKIPPED[{}]: {test} ({reason})", selected_transport());
        false
    }

    struct ImageFixture {
        _dir: tempfile::TempDir,
        global_config: PathBuf,
        image_config: PathBuf,
        virtual_size: u64,
    }

    async fn image_fixture(virtual_size: u64) -> ImageFixture {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let global_config = dir.path().join("global.json");
        std::fs::write(
            &global_config,
            serde_json::to_vec(&serde_json::json!({
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": cache_dir,
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": { "enable": false }
            }))
            .unwrap(),
        )
        .unwrap();

        let upper_data = dir.path().join("upper.data");
        overlaybd::helper::prepare_runtime_upper(
            &upper_data,
            None,
            virtual_size,
            OverlaybdUpperMode::Sparse,
        )
        .unwrap();
        let image_config = dir.path().join("image.json");
        std::fs::write(
            &image_config,
            serde_json::to_vec(&serde_json::json!({
                "lowers": [],
                "upper": { "mode": "sparse", "data": upper_data },
                "resultFile": dir.path().join("result.txt")
            }))
            .unwrap(),
        )
        .unwrap();

        ImageFixture {
            _dir: dir,
            global_config,
            image_config,
            virtual_size,
        }
    }

    async fn image_service(global_config: &Path) -> ImageService {
        ImageService::from_config_path(global_config).await.unwrap()
    }

    struct RunningDaemon {
        client: Arc<UblkDaemonClient>,
        server: Arc<UblkDaemonServer>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
        _dir: tempfile::TempDir,
        _transport: TestTransport,
    }

    impl RunningDaemon {
        async fn start(global_config: &Path, pool: Option<uvm_ublk_daemon::PoolConfig>) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let sock_path = dir.path().join("daemon.sock");
            let transport = test_transport();
            let mut server = UblkDaemonServer::new(
                sock_path.clone(),
                transport.handle(),
                image_service(global_config).await,
                dir.path().join("resize-overlaybd-global.json"),
            );
            let pool_enabled = pool.is_some();
            if let Some(pool) = pool {
                server.enable_pool(pool).await.unwrap();
            }
            let server = Arc::new(server);
            let task = {
                let server = Arc::clone(&server);
                tokio::spawn(async move { server.run().await })
            };
            for _ in 0..100 {
                if sock_path.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let client = UblkDaemonClient::new_for_test(sock_path, false);
            let _ = pool_enabled;
            Self {
                client,
                server,
                task,
                _dir: dir,
                _transport: transport,
            }
        }

        async fn stop(self) {
            self.server.request_shutdown();
            let _ = tokio::time::timeout(Duration::from_secs(30), self.task).await;
        }
    }

    /// The daemon's own formatter colours its fields, which would otherwise sit
    /// between a field name and its value.
    fn strip_ansi(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut chars = text.chars();
        while let Some(ch) = chars.next() {
            if ch != '\u{1b}' {
                out.push(ch);
                continue;
            }
            for escaped in chars.by_ref() {
                if escaped.is_ascii_alphabetic() {
                    break;
                }
            }
        }
        out
    }

    fn device_sectors(device_path: &Path) -> u64 {
        let name = device_path.file_name().unwrap().to_str().unwrap();
        std::fs::read_to_string(format!("/sys/block/{name}/size"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn a_created_device_serves_the_image_and_is_gone_after_delete() {
        let name = "a_created_device_serves_the_image_and_is_gone_after_delete";
        if !transport_reachable(name) {
            return;
        }
        let fixture = image_fixture(16 * 1024 * 1024).await;
        let daemon = RunningDaemon::start(&fixture.global_config, None).await;

        let (dev_id, device_path) = daemon
            .client
            .create_overlaybd(&fixture.image_config, &fixture.global_config)
            .await
            .expect("create the device");
        assert_eq!(
            device_sectors(&device_path) * 512,
            fixture.virtual_size,
            "the device must advertise the image's virtual size"
        );

        let payload: Vec<u8> = (0..4096u32).map(|index| (index % 251) as u8).collect();
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&device_path)
                .expect("open the device");
            file.write_all_at(&payload, 8192).expect("pwrite");
            file.sync_all().expect("fsync");
        }
        {
            let file = std::fs::File::open(&device_path).expect("reopen the device");
            let mut back = vec![0u8; 4096];
            file.read_exact_at(&mut back, 8192).expect("pread");
            assert_eq!(back, payload);
        }

        daemon.client.delete(dev_id).await.expect("delete");
        let name = device_path.file_name().unwrap().to_str().unwrap();
        let size = std::fs::read_to_string(format!("/sys/block/{name}/size"))
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok());
        assert!(
            matches!(size, None | Some(0)),
            "{} still reports {size:?} sectors after delete",
            device_path.display()
        );

        daemon.stop().await;
    }

    #[tokio::test]
    async fn a_released_device_is_reused_for_an_image_of_a_different_size() {
        let name = "a_released_device_is_reused_for_an_image_of_a_different_size";
        if !transport_reachable(name) {
            return;
        }
        let small = image_fixture(16 * 1024 * 1024).await;
        let large = image_fixture(48 * 1024 * 1024).await;
        let daemon = RunningDaemon::start(
            &small.global_config,
            Some(uvm_ublk_daemon::PoolConfig {
                low_watermark: 0,
                high_watermark: 2,
                maintenance_enabled: false,
                startup_prewarm: false,
            }),
        )
        .await;

        let (first_id, first_path) = daemon
            .client
            .acquire_overlaybd(
                &small.image_config,
                &small.global_config,
                small.virtual_size,
                AccessMode::Exclusive,
            )
            .await
            .expect("acquire the small image");
        assert_eq!(device_sectors(&first_path) * 512, small.virtual_size);
        daemon
            .client
            .release_overlaybd(first_id)
            .await
            .expect("release");

        let (second_id, second_path) = daemon
            .client
            .acquire_overlaybd(
                &large.image_config,
                &large.global_config,
                large.virtual_size,
                AccessMode::Exclusive,
            )
            .await
            .expect("acquire the large image");
        assert_eq!(
            second_id, first_id,
            "a transport that can resize must reuse the idle device rather than create one"
        );
        assert_eq!(
            device_sectors(&second_path) * 512,
            large.virtual_size,
            "the reused device must advertise the new image's size"
        );

        let mut tail = vec![0u8; 4096];
        let file = std::fs::File::open(&second_path).expect("open the reused device");
        file.read_exact_at(&mut tail, large.virtual_size - 4096)
            .expect("a read past the previous image's end must be served");
        drop(file);

        daemon
            .client
            .release_overlaybd(second_id)
            .await
            .expect("release");
        daemon.stop().await;
    }

    #[tokio::test]
    async fn the_spawned_daemon_carries_the_dead_connection_timeout_from_its_cli() {
        let name = "the_spawned_daemon_carries_the_dead_connection_timeout_from_its_cli";
        if !transport_reachable(name) {
            return;
        }
        let fixture = image_fixture(16 * 1024 * 1024).await;
        let dir = tempfile::tempdir().unwrap();
        let log_file = dir.path().join("daemon.log");
        let socket_path = dir.path().join("daemon.sock");

        let client = UblkDaemonClient::new(uvm_ublk_daemon::UblkDaemonSpawnConfig {
            binary_path: Path::new(env!("CARGO_BIN_EXE_uvm-ublk-daemon")),
            socket_path,
            global_config: &fixture.global_config,
            resize_global_config: &fixture.global_config,
            app_config: None,
            log_file: Some(&log_file),
            metrics_listen_addr: "",
            pool_config: None,
            p2p_publish_url: None,
            runtime_device_timeout: Duration::from_secs(120),
            transport: selected_transport(),
            nbd_connections: 2,
            nbd_io_timeout_secs: 45,
            nbd_dead_conn_timeout_secs: 17,
        })
        .await
        .expect("spawn the daemon");

        let (dev_id, device_path) = client
            .create_overlaybd(&fixture.image_config, &fixture.global_config)
            .await
            .expect("create a device through the spawned daemon");
        assert_eq!(device_sectors(&device_path) * 512, fixture.virtual_size);

        let log = strip_ansi(&std::fs::read_to_string(&log_file).expect("read the daemon log"));
        assert!(
            log.contains("nbd_dead_conn_timeout_secs=17"),
            "the daemon did not report the timeout its CLI was given; log was:\n{log}"
        );
        assert!(log.contains("nbd_io_timeout_secs=45"), "log was:\n{log}");

        client.delete(dev_id).await.expect("delete");
        client.shutdown().await.expect("shut the daemon down");
    }

    #[tokio::test]
    async fn a_shared_device_is_refcounted_across_two_acquires() {
        let name = "a_shared_device_is_refcounted_across_two_acquires";
        if !transport_reachable(name) {
            return;
        }
        let fixture = image_fixture(16 * 1024 * 1024).await;
        let daemon = RunningDaemon::start(
            &fixture.global_config,
            Some(uvm_ublk_daemon::PoolConfig {
                low_watermark: 0,
                high_watermark: 2,
                maintenance_enabled: false,
                startup_prewarm: false,
            }),
        )
        .await;

        let (first_id, _) = daemon
            .client
            .acquire_overlaybd(
                &fixture.image_config,
                &fixture.global_config,
                fixture.virtual_size,
                AccessMode::Shared,
            )
            .await
            .expect("first shared acquire");
        let (second_id, _) = daemon
            .client
            .acquire_overlaybd(
                &fixture.image_config,
                &fixture.global_config,
                fixture.virtual_size,
                AccessMode::Shared,
            )
            .await
            .expect("second shared acquire");
        assert_eq!(first_id, second_id, "one image, one shared device");

        daemon.client.release_overlaybd(first_id).await.unwrap();
        daemon.client.release_overlaybd(second_id).await.unwrap();
        daemon.stop().await;
    }
    // ── Memory servers over userfaultfd ─────────────────────────────────

    mod memory_uffd_tests {
        use super::*;
        use std::fs::OpenOptions;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use uvm_ublk_daemon::MemoryUffdServeOptions;
        use uvm_uffd::testing::{create_uffd_for_test, AnonRegion};
        use uvm_uffd::{send_handshake, PrefetchList, Uffd};

        const PAGE: usize = 4096;
        const IMAGE_SIZE: u64 = 8 * 1024 * 1024;

        /// Playing the VMM side needs a userfaultfd of our own;
        /// `AENV_UFFD_TEST_REQUIRED=1` turns a skip into a failure.
        fn uffd_or_skip(test: &str) -> Option<Uffd> {
            if let Some((uffd, _)) = create_uffd_for_test() {
                return Some(uffd);
            }
            let reason = "cannot create a userfaultfd (needs CAP_SYS_PTRACE, vm.unprivileged_userfaultfd=1 or /dev/userfaultfd)";
            if std::env::var("AENV_UFFD_TEST_REQUIRED").as_deref() == Ok("1") {
                panic!("AENV_UFFD_TEST_REQUIRED=1 but {test} cannot run: {reason}");
            }
            eprintln!("SKIPPED[uffd]: {test} ({reason})");
            None
        }

        /// Distinct non-zero bytes, so a served page is a copy and never a
        /// zero page.
        fn page_content(index: usize) -> Vec<u8> {
            (0..PAGE)
                .map(|byte| ((byte + index * 7) % 251 + 1) as u8)
                .collect()
        }

        async fn wait_for_state(
            client: &UblkDaemonClient,
            serve_id: u32,
            wanted: MemoryUffdState,
        ) -> MemoryUffdState {
            let mut state = MemoryUffdState::Starting;
            for _ in 0..250 {
                state = client
                    .query_memory_uffd(serve_id)
                    .await
                    .expect("query")
                    .state;
                if state == wanted {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            state
        }

        #[tokio::test]
        async fn a_serve_that_never_gets_a_handshake_exits_within_its_timeout_and_stops_cleanly() {
            let name =
                "a_serve_that_never_gets_a_handshake_exits_within_its_timeout_and_stops_cleanly";
            if !transport_reachable(name) {
                return;
            }
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;
            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("mem.sock");
            let serve_id = daemon
                .client
                .serve_memory_uffd(
                    &fixture.image_config,
                    &fixture.global_config,
                    &socket_path,
                    MemoryUffdServeOptions {
                        source_block_bytes: 4 * 1024 * 1024,
                        source_cache_bytes: 32 * 1024 * 1024,
                        max_inflight: 16,
                        read_retry_secs: 5,
                        handshake_timeout_secs: 1,
                    },
                    None,
                )
                .await
                .expect("serve");
            assert!(
                socket_path.exists(),
                "the socket is bound before anyone connects"
            );

            let mut state = MemoryUffdState::Starting;
            for _ in 0..250 {
                state = daemon
                    .client
                    .query_memory_uffd(serve_id)
                    .await
                    .expect("query")
                    .state;
                if matches!(state, MemoryUffdState::Exited { .. }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            match state {
                MemoryUffdState::Exited { error: Some(error) } => {
                    assert!(error.contains("no uffd handshake"), "{error}");
                }
                other => panic!("the server did not report its handshake timeout: {other:?}"),
            }
            // The exited server is still addressable until it is stopped,
            // and stopping it is the ordinary path.
            daemon
                .client
                .stop_memory_uffd(serve_id)
                .await
                .expect("stop an exited server");
            assert!(!socket_path.exists(), "the socket is unlinked at stop");
            assert!(daemon.client.query_memory_uffd(serve_id).await.is_err());
        }

        #[tokio::test]
        async fn serve_memory_uffd_fills_guest_pages_from_the_image() {
            let name = "serve_memory_uffd_fills_guest_pages_from_the_image";
            if !transport_reachable(name) {
                return;
            }
            let Some(uffd) = uffd_or_skip(name) else {
                return;
            };
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;

            // Seed known bytes through a device; the memory server reads the
            // same image back once the device is gone.
            let pages: Vec<(usize, Vec<u8>)> = [0usize, 1, 17, 129]
                .into_iter()
                .map(|index| (index, page_content(index)))
                .collect();
            let (dev_id, device_path) = daemon
                .client
                .create_overlaybd(&fixture.image_config, &fixture.global_config)
                .await
                .expect("create the seeding device");
            {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&device_path)
                    .expect("open the seeding device");
                for (index, content) in &pages {
                    file.write_all_at(content, (index * PAGE) as u64)
                        .expect("pwrite");
                }
                file.sync_all().expect("fsync");
            }
            daemon.client.delete(dev_id).await.expect("delete");

            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("mem.sock");
            let serve_id = daemon
                .client
                .serve_memory_uffd(
                    &fixture.image_config,
                    &fixture.global_config,
                    &socket_path,
                    MemoryUffdServeOptions {
                        source_block_bytes: 4 * 1024 * 1024,
                        source_cache_bytes: 32 * 1024 * 1024,
                        max_inflight: 16,
                        read_retry_secs: 5,
                        handshake_timeout_secs: 60,
                    },
                    None,
                )
                .await
                .expect("serve the memory image");

            let region = Arc::new(AnonRegion::new(IMAGE_SIZE as usize).expect("map the region"));
            region.register(&uffd, 0).expect("register the region");
            let stream = UnixStream::connect(&socket_path).expect("connect to the memory server");
            send_handshake(
                &stream,
                &[region.mapping(0, PAGE as u64)],
                &[uffd.as_raw_fd()],
            )
            .expect("send the handshake");
            assert_eq!(
                wait_for_state(&daemon.client, serve_id, MemoryUffdState::Serving).await,
                MemoryUffdState::Serving
            );
            // The server owns the descriptor it was handed.
            drop(uffd);

            let offsets: Vec<usize> = pages.iter().map(|(index, _)| index * PAGE).collect();
            let faulted = {
                let region = Arc::clone(&region);
                tokio::task::spawn_blocking(move || {
                    offsets
                        .into_iter()
                        .map(|at| region.read(at, PAGE))
                        .collect::<Vec<_>>()
                })
                .await
                .expect("fault the guest pages in")
            };
            for ((index, content), got) in pages.iter().zip(faulted) {
                assert_eq!(&got, content, "page {index} came back with other bytes");
            }

            let status = daemon
                .client
                .query_memory_uffd(serve_id)
                .await
                .expect("query the counters");
            // The test registered MISSING only, and the handler says so.
            assert_eq!(status.write_protect, Some(false));
            assert_eq!(status.regions.len(), 1);
            assert_eq!(status.regions[0].size, region.len() as u64);
            let stats = status.stats;
            assert!(
                stats.pages_copied > 0 && stats.faults >= pages.len() as u64,
                "{stats:?}"
            );

            daemon
                .client
                .stop_memory_uffd(serve_id)
                .await
                .expect("stop the memory server");
            assert!(
                daemon.client.query_memory_uffd(serve_id).await.is_err(),
                "a stopped serve id must be unknown"
            );
            assert!(
                !socket_path.exists(),
                "stopping the memory server must remove its socket"
            );
            daemon.stop().await;
        }

        #[tokio::test]
        async fn two_uffd_servers_share_one_opened_image() {
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;
            let socket_dir = tempfile::tempdir().unwrap();

            let mut serve_ids = Vec::new();
            for name in ["first.sock", "second.sock"] {
                serve_ids.push(
                    daemon
                        .client
                        .serve_memory_uffd(
                            &fixture.image_config,
                            &fixture.global_config,
                            &socket_dir.path().join(name),
                            MemoryUffdServeOptions {
                                source_block_bytes: 4 * 1024 * 1024,
                                source_cache_bytes: 32 * 1024 * 1024,
                                max_inflight: 16,
                                read_retry_secs: 5,
                                handshake_timeout_secs: 60,
                            },
                            None,
                        )
                        .await
                        .expect("serve the memory image"),
                );
            }
            assert_ne!(serve_ids[0], serve_ids[1], "each server gets its own id");
            assert_eq!(
                daemon.server.memory_uffd_open_images(),
                1,
                "one image opened for both servers"
            );

            daemon.client.stop_memory_uffd(serve_ids[0]).await.unwrap();
            assert_eq!(
                daemon.server.memory_uffd_open_images(),
                1,
                "the image stays open while the second server reads it"
            );
            daemon.client.stop_memory_uffd(serve_ids[1]).await.unwrap();
            assert_eq!(
                daemon.server.memory_uffd_open_images(),
                0,
                "the last stop closes the image"
            );
            daemon.stop().await;
        }

        #[tokio::test]
        async fn stop_memory_uffd_with_an_unknown_id_is_an_error() {
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;

            let err = daemon
                .client
                .stop_memory_uffd(4242)
                .await
                .expect_err("an unknown serve id has nothing to stop");
            assert!(format!("{err:#}").contains("4242"), "{err:#}");
            assert!(daemon.client.query_memory_uffd(4242).await.is_err());

            daemon.stop().await;
        }

        #[tokio::test]
        async fn a_resume_records_its_working_set_and_the_next_one_prefaults_it() {
            let name = "a_resume_records_its_working_set_and_the_next_one_prefaults_it";
            if !transport_reachable(name) {
                return;
            }
            let Some(uffd) = uffd_or_skip(name) else {
                return;
            };
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;
            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("mem.sock");
            let prefetch_path = socket_dir.path().join("mem_prefetch.json");
            let pages: Vec<usize> = (0..12).map(|i| i * 3).collect();

            let serve_id = daemon
                .client
                .serve_memory_uffd(
                    &fixture.image_config,
                    &fixture.global_config,
                    &socket_path,
                    MemoryUffdServeOptions {
                        source_block_bytes: 4 * 1024 * 1024,
                        source_cache_bytes: 32 * 1024 * 1024,
                        max_inflight: 16,
                        read_retry_secs: 5,
                        handshake_timeout_secs: 60,
                    },
                    Some(&prefetch_path),
                )
                .await
                .expect("serve the memory image");
            {
                let region =
                    Arc::new(AnonRegion::new(IMAGE_SIZE as usize).expect("map the region"));
                region.register(&uffd, 0).expect("register the region");
                let stream = UnixStream::connect(&socket_path).expect("connect");
                send_handshake(
                    &stream,
                    &[region.mapping(0, PAGE as u64)],
                    &[uffd.as_raw_fd()],
                )
                .expect("send the handshake");
                assert_eq!(
                    wait_for_state(&daemon.client, serve_id, MemoryUffdState::Serving).await,
                    MemoryUffdState::Serving
                );
                drop(uffd);
                let faulter = Arc::clone(&region);
                let touched = pages.clone();
                tokio::task::spawn_blocking(move || {
                    for p in touched {
                        let _ = faulter.read_byte(p * PAGE);
                    }
                })
                .await
                .expect("fault the pages in");
                daemon
                    .client
                    .stop_memory_uffd(serve_id)
                    .await
                    .expect("stop");
            }
            let list = PrefetchList::read(&prefetch_path)
                .expect("read the list")
                .expect("the first resume recorded its working set");
            assert_eq!(
                list.pages,
                pages.iter().map(|p| *p as u64).collect::<Vec<_>>()
            );

            let Some(uffd) = uffd_or_skip(name) else {
                return;
            };
            let serve_id = daemon
                .client
                .serve_memory_uffd(
                    &fixture.image_config,
                    &fixture.global_config,
                    &socket_path,
                    MemoryUffdServeOptions {
                        source_block_bytes: 4 * 1024 * 1024,
                        source_cache_bytes: 32 * 1024 * 1024,
                        max_inflight: 16,
                        read_retry_secs: 5,
                        handshake_timeout_secs: 60,
                    },
                    Some(&prefetch_path),
                )
                .await
                .expect("serve the memory image again");
            let region = Arc::new(AnonRegion::new(IMAGE_SIZE as usize).expect("map the region"));
            region.register(&uffd, 0).expect("register the region");
            let stream = UnixStream::connect(&socket_path).expect("connect");
            send_handshake(
                &stream,
                &[region.mapping(0, PAGE as u64)],
                &[uffd.as_raw_fd()],
            )
            .expect("send the handshake");
            assert_eq!(
                wait_for_state(&daemon.client, serve_id, MemoryUffdState::Serving).await,
                MemoryUffdState::Serving
            );
            drop(uffd);
            let mut stats = MemoryUffdStats::default();
            for _ in 0..250 {
                stats = daemon
                    .client
                    .query_memory_uffd(serve_id)
                    .await
                    .expect("query")
                    .stats;
                if stats.prefaulted as usize == pages.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(stats.prefaulted as usize, pages.len(), "{stats:?}");

            let faulter = Arc::clone(&region);
            let touched = pages.clone();
            tokio::task::spawn_blocking(move || {
                for p in touched {
                    let _ = faulter.read_byte(p * PAGE);
                }
            })
            .await
            .expect("read the prefaulted pages");
            let after = daemon
                .client
                .query_memory_uffd(serve_id)
                .await
                .expect("query")
                .stats;
            assert_eq!(
                after.faults, stats.faults,
                "prefaulted pages are present and do not fault"
            );
            daemon
                .client
                .stop_memory_uffd(serve_id)
                .await
                .expect("stop");
            assert_eq!(
                PrefetchList::read(&prefetch_path).unwrap().unwrap(),
                list,
                "a later resume does not rewrite the list"
            );
            daemon.stop().await;
        }

        #[tokio::test]
        async fn shutdown_stops_uffd_servers() {
            let fixture = image_fixture(IMAGE_SIZE).await;
            let daemon = RunningDaemon::start(&fixture.global_config, None).await;
            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("mem.sock");

            daemon
                .client
                .serve_memory_uffd(
                    &fixture.image_config,
                    &fixture.global_config,
                    &socket_path,
                    MemoryUffdServeOptions {
                        source_block_bytes: 4 * 1024 * 1024,
                        source_cache_bytes: 32 * 1024 * 1024,
                        max_inflight: 16,
                        read_retry_secs: 5,
                        handshake_timeout_secs: 60,
                    },
                    None,
                )
                .await
                .expect("serve the memory image");
            assert_eq!(daemon.server.memory_uffd_open_images(), 1);

            let server = Arc::clone(&daemon.server);
            daemon.stop().await;

            assert_eq!(
                server.memory_uffd_open_images(),
                0,
                "shutdown closed the memory image"
            );
            assert!(
                !socket_path.exists(),
                "shutdown removed the memory server socket"
            );
        }
    }
}
