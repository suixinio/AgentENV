//! The daemon's userfaultfd memory servers: one per restored VM, reading an
//! overlaybd memory image the daemon opens once per (image config, global
//! config) and every server on that image shares.

use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use overlaybd::image_file::ImageFile;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uvm_uffd::{
    HandlerOptions, HandlerState, OverlaybdSource, PrefetchList, StatsSnapshot, UffdHandler,
};

use crate::protocol::{DaemonResponse, MemoryUffdRegion, MemoryUffdState, MemoryUffdStats};
use crate::server::ImageServiceCache;

/// Canonical key for a shared opened memory image: (image_config, global_config).
type ImageKey = (PathBuf, PathBuf);

/// One opened memory image, refcounted by the servers reading it.
struct SharedImage {
    image: Arc<ImageFile>,
    refcount: usize,
}

struct ActiveServe {
    /// The watcher task holds the second reference; a stop joins the watcher
    /// before it takes the handler back to join its thread.
    handler: Arc<UffdHandler>,
    watcher: JoinHandle<()>,
    cancel_watcher: oneshot::Sender<()>,
    socket_path: PathBuf,
    image_key: ImageKey,
    /// The image size the prefetch list is keyed by.
    image_size: u64,
    /// Where the working set is recorded at stop, unless a list exists.
    prefetch_path: Option<PathBuf>,
}

pub(crate) struct ServeMemoryUffdRequest<'a> {
    pub(crate) image_config: &'a Path,
    pub(crate) global_config: &'a Path,
    pub(crate) socket_path: &'a Path,
    pub(crate) max_inflight: usize,
    pub(crate) read_retry_secs: u64,
    pub(crate) handshake_timeout_secs: u64,
    pub(crate) prefetch_path: Option<&'a Path>,
}

/// Every userfaultfd memory server this daemon owns.
pub(crate) struct MemoryUffdServers {
    next_serve_id: AtomicU32,
    serves: DashMap<u32, ActiveServe>,
    images: DashMap<ImageKey, SharedImage>,
}

impl MemoryUffdServers {
    pub(crate) fn new() -> Self {
        Self {
            // Serve ids start at one, so zero is never a live server.
            next_serve_id: AtomicU32::new(1),
            serves: DashMap::new(),
            images: DashMap::new(),
        }
    }

    /// Memory images currently open for userfaultfd serving; the servers
    /// sharing one image count once.
    pub(crate) fn open_images(&self) -> usize {
        self.images.len()
    }

    pub(crate) async fn serve(
        &self,
        image_service_cache: &ImageServiceCache,
        request: ServeMemoryUffdRequest<'_>,
    ) -> Result<DaemonResponse> {
        let key: ImageKey = (
            request.image_config.to_path_buf(),
            request.global_config.to_path_buf(),
        );
        let image = self.acquire_image(image_service_cache, &key).await?;
        match self.start_serve(&request, &key, image) {
            Ok(serve_id) => Ok(DaemonResponse::MemoryUffdServing { serve_id }),
            Err(err) => {
                self.release_image(&key);
                Err(err)
            }
        }
    }

    pub(crate) async fn stop(&self, serve_id: u32) -> Result<DaemonResponse> {
        let Some((_, serve)) = self.serves.remove(&serve_id) else {
            bail!("memory uffd server {serve_id} not found");
        };
        self.shutdown_serve(serve_id, serve).await;
        Ok(DaemonResponse::Ok)
    }

    pub(crate) fn query(&self, serve_id: u32) -> Result<DaemonResponse> {
        let Some(serve) = self.serves.get(&serve_id) else {
            bail!("memory uffd server {serve_id} not found");
        };
        Ok(DaemonResponse::MemoryUffdStatus {
            state: state_of(serve.handler.state()),
            stats: stats_of(serve.handler.stats()),
            write_protect: serve.handler.write_protect(),
            regions: serve
                .handler
                .regions()
                .into_iter()
                .map(|m| MemoryUffdRegion {
                    host_addr: m.base_host_virt_addr,
                    size: m.size,
                    offset: m.offset,
                    page_size: m.page_size(),
                })
                .collect(),
        })
    }

    /// Stops every server; a guest still faulting on one is no longer backed.
    pub(crate) async fn stop_all(&self) {
        let serve_ids: Vec<u32> = self.serves.iter().map(|entry| *entry.key()).collect();
        for serve_id in serve_ids {
            if let Some((_, serve)) = self.serves.remove(&serve_id) {
                tracing::info!(serve_id, "stopping memory uffd server during shutdown");
                self.shutdown_serve(serve_id, serve).await;
            }
        }
    }

    fn start_serve(
        &self,
        request: &ServeMemoryUffdRequest<'_>,
        key: &ImageKey,
        image: Arc<ImageFile>,
    ) -> Result<u32> {
        let source = Arc::new(
            OverlaybdSource::from_opened_image(request.image_config.to_path_buf(), image)
                .context("build the page source for the memory image")?,
        );
        let image_size = source.image().size_bytes();
        let listener = bind_handshake_socket(request.socket_path)?;
        let serve_id = loop {
            let id = self.next_serve_id.fetch_add(1, Ordering::Relaxed);
            // Zero is never a live server, and a wrapped counter must not
            // land on one that still is.
            if id != 0 && !self.serves.contains_key(&id) {
                break id;
            }
        };
        let started = UffdHandler::serve_socket(
            listener,
            source,
            HandlerOptions {
                max_inflight: request.max_inflight,
                read_retry_budget: Duration::from_secs(request.read_retry_secs),
                handshake_timeout: Duration::from_secs(request.handshake_timeout_secs),
                name: format!("mem-{serve_id}"),
                ..Default::default()
            },
        );
        let handler = match started {
            Ok(handler) => Arc::new(handler),
            Err(err) => {
                let _ = std::fs::remove_file(request.socket_path);
                return Err(err.context(format!(
                    "start the memory uffd server on {}",
                    request.socket_path.display()
                )));
            }
        };

        let prefetch = request
            .prefetch_path
            .map(|path| (path.to_path_buf(), read_prefetch_list(path)));
        let (cancel_watcher, cancelled) = oneshot::channel();
        let watcher = tokio::spawn({
            let handler = Arc::clone(&handler);
            let image_config = request.image_config.to_path_buf();
            let replay = prefetch.as_ref().and_then(|(_, list)| list.clone());
            async move {
                let serve = async {
                    if let Some(list) = replay {
                        replay_prefetch(serve_id, &handler, list, image_size).await;
                    }
                    handler.wait_exit().await
                };
                tokio::select! {
                    exit = serve => match exit {
                        Some(error) => {
                            // The guest is unbacked from here on; the node
                            // sees it at its next query. Loud on both
                            // channels the node does not read.
                            metrics::counter!("uffd_memory_server_exits_total", "outcome" => "error")
                                .increment(1);
                            tracing::error!(
                                serve_id,
                                image_config = %image_config.display(),
                                error = %error,
                                stats = ?handler.stats(),
                                "memory uffd server exited with an error"
                            )
                        }
                        None => {
                            metrics::counter!("uffd_memory_server_exits_total", "outcome" => "ok")
                                .increment(1);
                            tracing::info!(
                                serve_id,
                                image_config = %image_config.display(),
                                "memory uffd server exited"
                            )
                        }
                    },
                    _ = cancelled => {}
                }
            }
        });

        self.serves.insert(
            serve_id,
            ActiveServe {
                handler,
                watcher,
                cancel_watcher,
                socket_path: request.socket_path.to_path_buf(),
                image_key: key.clone(),
                image_size,
                prefetch_path: prefetch.map(|(path, _)| path),
            },
        );
        metrics::gauge!("uffd_memory_servers").set(self.serves.len() as f64);
        tracing::info!(
            serve_id,
            image_config = %request.image_config.display(),
            socket = %request.socket_path.display(),
            max_inflight = request.max_inflight,
            "serving a memory image over userfaultfd"
        );
        Ok(serve_id)
    }

    async fn shutdown_serve(&self, serve_id: u32, serve: ActiveServe) {
        let ActiveServe {
            handler,
            watcher,
            cancel_watcher,
            socket_path,
            image_key,
            image_size,
            prefetch_path,
        } = serve;

        // Joining the watcher leaves this the only reference to the handler,
        // which the blocking join below needs to own.
        let _ = cancel_watcher.send(());
        let _ = watcher.await;
        if let Some(path) = prefetch_path {
            record_prefetch_list(serve_id, &handler, &path, image_size);
        }
        let stats = handler.stats();
        record_final_stats(&stats);
        tracing::info!(serve_id, ?stats, "memory uffd server final counters");
        // Stopping is asked for before the ownership check, so a handler
        // some other reference still holds is told to stop rather than
        // left serving a socket that is about to be unlinked.
        handler.request_stop();
        match Arc::into_inner(handler) {
            Some(handler) => match tokio::task::spawn_blocking(move || handler.stop()).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => tracing::error!(
                    serve_id,
                    error = %format!("{err:#}"),
                    "memory uffd server ended with an error"
                ),
                Err(err) => tracing::error!(
                    serve_id,
                    error = %err,
                    "joining the memory uffd server thread failed"
                ),
            },
            None => tracing::error!(
                serve_id,
                "the memory uffd handler is still shared; stopping it without a join"
            ),
        }

        if let Err(err) = std::fs::remove_file(&socket_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    serve_id,
                    path = %socket_path.display(),
                    error = %err,
                    "removing the memory uffd socket failed"
                );
            }
        }
        self.release_image(&image_key);
        metrics::gauge!("uffd_memory_servers").set(self.serves.len() as f64);
        tracing::info!(serve_id, "memory uffd server stopped");
    }

    async fn acquire_image(
        &self,
        image_service_cache: &ImageServiceCache,
        key: &ImageKey,
    ) -> Result<Arc<ImageFile>> {
        let (image_config, global_config) = key;
        if let Some(mut shared) = self.images.get_mut(key) {
            shared.refcount += 1;
            tracing::debug!(
                image_config = %image_config.display(),
                refcount = shared.refcount,
                "reusing the opened memory image"
            );
            return Ok(Arc::clone(&shared.image));
        }

        let image_service = image_service_cache
            .get_or_create(global_config)
            .await
            .context("resolve image service for the memory image")?;
        // The image config path is the download gate's device key, so
        // NotifySandboxReady releases the held background downloads of an
        // image served over userfaultfd as it does for a device.
        let image = Arc::new(
            image_service
                .create_image_file(image_config)
                .await
                .with_context(|| {
                    format!("open overlaybd memory image: {}", image_config.display())
                })?,
        );

        match self.images.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let shared = entry.get_mut();
                shared.refcount += 1;
                let existing = Arc::clone(&shared.image);
                drop(entry);
                drop(image);
                tracing::debug!(
                    image_config = %image_config.display(),
                    "a concurrent serve opened the same memory image first"
                );
                Ok(existing)
            }
            Entry::Vacant(entry) => {
                entry.insert(SharedImage {
                    image: Arc::clone(&image),
                    refcount: 1,
                });
                Ok(image)
            }
        }
    }

    fn release_image(&self, key: &ImageKey) {
        let (image_config, _) = key;
        let mut closed = None;
        if let Entry::Occupied(mut entry) = self.images.entry(key.clone()) {
            let shared = entry.get_mut();
            shared.refcount = shared.refcount.saturating_sub(1);
            if shared.refcount == 0 {
                closed = Some(entry.remove());
            }
        }
        if closed.is_some() {
            tracing::debug!(
                image_config = %image_config.display(),
                "closed the memory image behind the last userfaultfd server"
            );
        }
    }
}

/// The socket Firecracker connects to. A file left behind by an earlier server
/// is removed first; the parent directory must already exist.
fn bind_handshake_socket(socket_path: &Path) -> Result<UnixListener> {
    if let Err(err) = std::fs::remove_file(socket_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err).with_context(|| {
                format!(
                    "remove the stale memory uffd socket {}",
                    socket_path.display()
                )
            });
        }
    }
    UnixListener::bind(socket_path)
        .with_context(|| format!("bind the memory uffd socket {}", socket_path.display()))
}

fn read_prefetch_list(path: &Path) -> Option<PrefetchList> {
    match PrefetchList::read(path) {
        Ok(list) => list,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %format!("{err:#}"),
                "ignoring an unreadable memory prefetch list"
            );
            None
        }
    }
}

/// Adds a server's final counters to the daemon-wide totals the metrics
/// endpoint exports.
fn record_final_stats(stats: &StatsSnapshot) {
    let totals = [
        ("uffd_memory_faults_total", stats.faults),
        ("uffd_memory_pages_copied_total", stats.pages_copied),
        ("uffd_memory_pages_zeroed_total", stats.pages_zeroed),
        ("uffd_memory_bytes_read_total", stats.bytes_read),
        ("uffd_memory_read_retries_total", stats.read_retries),
        ("uffd_memory_removes_total", stats.removes),
        ("uffd_memory_prefaulted_total", stats.prefaulted),
        ("uffd_memory_unmapped_faults_total", stats.unmapped),
    ];
    for (name, value) in totals {
        metrics::counter!(name).increment(value);
    }
}

/// Installs the recorded working set once the handshake has fixed the page
/// size; a list for another page size or image size is left alone.
async fn replay_prefetch(
    serve_id: u32,
    handler: &UffdHandler,
    list: PrefetchList,
    image_size: u64,
) {
    if handler.wait_serving().await.is_err() {
        return;
    }
    let page_size = handler.page_size().unwrap_or(0);
    match list.pages_for(page_size, image_size) {
        Some(pages) => {
            tracing::info!(
                serve_id,
                pages = pages.len(),
                "prefaulting the recorded working set"
            );
            if let Err(err) = handler.prefault(pages.to_vec()) {
                tracing::warn!(serve_id, error = %err, "prefault request was not accepted");
            }
        }
        None => tracing::warn!(
            serve_id,
            recorded_page_size = list.page_size,
            recorded_image_size = list.image_size,
            page_size,
            image_size,
            "ignoring a memory prefetch list recorded for another page size or image"
        ),
    }
}

/// Records the pages this server installed as the image's working set,
/// unless a list exists already: the first resume of an image wins.
fn record_prefetch_list(serve_id: u32, handler: &UffdHandler, path: &Path, image_size: u64) {
    let Some(page_size) = handler.page_size() else {
        return;
    };
    let list = PrefetchList::new(page_size, image_size, handler.faulted_pages());
    if !list.is_worth_recording() {
        return;
    }
    match list.write_if_absent(path) {
        Ok(true) => tracing::info!(
            serve_id,
            pages = list.pages.len(),
            path = %path.display(),
            "recorded the memory working set"
        ),
        Ok(false) => {}
        Err(err) => tracing::warn!(
            serve_id,
            path = %path.display(),
            error = %format!("{err:#}"),
            "recording the memory working set failed"
        ),
    }
}

fn state_of(state: HandlerState) -> MemoryUffdState {
    match state {
        HandlerState::Starting => MemoryUffdState::Starting,
        HandlerState::Serving => MemoryUffdState::Serving,
        HandlerState::Exited(error) => MemoryUffdState::Exited { error },
    }
}

fn stats_of(stats: StatsSnapshot) -> MemoryUffdStats {
    MemoryUffdStats {
        faults: stats.faults,
        pages_copied: stats.pages_copied,
        pages_zeroed: stats.pages_zeroed,
        already_present: stats.already_present,
        duplicates: stats.duplicates,
        bytes_read: stats.bytes_read,
        read_retries: stats.read_retries,
        copy_retries: stats.copy_retries,
        removes: stats.removes,
        prefaulted: stats.prefaulted,
        unmapped: stats.unmapped,
        wp_faults: stats.wp_faults,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handler_state_maps_onto_the_wire_state() {
        assert_eq!(state_of(HandlerState::Starting), MemoryUffdState::Starting);
        assert_eq!(state_of(HandlerState::Serving), MemoryUffdState::Serving);
        assert_eq!(
            state_of(HandlerState::Exited(None)),
            MemoryUffdState::Exited { error: None }
        );
        assert_eq!(
            state_of(HandlerState::Exited(Some("read failed".to_string()))),
            MemoryUffdState::Exited {
                error: Some("read failed".to_string())
            }
        );
    }

    #[test]
    fn every_wire_counter_carries_the_handler_counter_of_the_same_name() {
        let stats = StatsSnapshot {
            faults: 1,
            pages_copied: 2,
            pages_zeroed: 3,
            already_present: 4,
            duplicates: 5,
            bytes_read: 6,
            read_retries: 7,
            copy_retries: 8,
            removes: 9,
            prefaulted: 10,
            unmapped: 11,
            wp_faults: 12,
        };
        assert_eq!(
            stats_of(stats),
            MemoryUffdStats {
                faults: 1,
                pages_copied: 2,
                pages_zeroed: 3,
                already_present: 4,
                duplicates: 5,
                bytes_read: 6,
                read_retries: 7,
                copy_retries: 8,
                removes: 9,
                prefaulted: 10,
                unmapped: 11,
                wp_faults: 12,
            }
        );
    }

    #[test]
    fn binding_replaces_a_socket_an_earlier_server_left_behind() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("mem.sock");
        let stale = bind_handshake_socket(&socket_path).expect("bind");
        drop(stale);
        assert!(socket_path.exists());

        let listener = bind_handshake_socket(&socket_path).expect("rebind over the stale socket");
        assert!(listener.local_addr().is_ok());
    }

    #[test]
    fn binding_under_a_missing_directory_fails() {
        let dir = tempfile::tempdir().unwrap();
        let err = bind_handshake_socket(&dir.path().join("absent").join("mem.sock"))
            .expect_err("the parent directory must already exist");
        assert!(format!("{err:#}").contains("bind the memory uffd socket"));
    }
}
