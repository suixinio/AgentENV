//! Hot-reloadable scheduler channel shared by node-side consumers.
//!
//! Static endpoint errors fail construction. Runtime reload errors keep the
//! previous channel. A background watcher performs file I/O; request-path reads
//! use a cheap watch receiver.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};
use tracing::{error, info, warn};

use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};

/// Scheduler-endpoint reload metric labeled by component and outcome.
pub const SCHEDULER_ENDPOINT_RELOAD_METRIC: &str = "agentenv_scheduler_endpoint_reload_total";

/// Shared live channel and its qualified endpoint string.
#[derive(Clone)]
pub struct SchedulerEndpointSource {
    rx: watch::Receiver<(Channel, String)>,
}

impl SchedulerEndpointSource {
    /// Builds a static channel and optionally spawns a permanent file watcher.
    ///
    /// Runtime reload failures retain the last working channel.
    pub fn spawn(
        static_endpoint: String,
        file_watch: Option<(PathBuf, Duration)>,
        component: &'static str,
    ) -> Result<Self> {
        // Build and publish the same qualified endpoint string.
        let (channel, static_endpoint) = build_channel(&static_endpoint)?;
        let (tx, rx) = watch::channel((channel, static_endpoint));

        if let Some((path, interval)) = file_watch {
            tokio::spawn(watch_file(path, interval, component, tx));
        }

        Ok(Self { rx })
    }

    /// Spawns using the configured endpoint file and heartbeat cadence.
    pub fn spawn_from_config(
        static_endpoint: String,
        cluster: &ClusterConfig,
        scheduler_report: &ObservabilitySchedulerReportConfig,
        component: &'static str,
    ) -> Result<Self> {
        let file = resolve_endpoint_file(cluster);
        let interval = Duration::from_secs(scheduler_report.interval_secs.max(1));
        Self::spawn(
            static_endpoint,
            file.map(|path| (path, interval)),
            component,
        )
    }

    /// Returns the current channel and endpoint without file I/O.
    pub fn current(&self) -> (Channel, String) {
        self.rx.borrow().clone()
    }

    /// Returns the current channel without its endpoint string.
    pub fn channel(&self) -> Channel {
        self.rx.borrow().0.clone()
    }

    /// Test source that permanently returns a prebuilt channel.
    #[cfg(test)]
    pub fn fixed(channel: Channel, endpoint: impl Into<String>) -> Self {
        let (_tx, rx) = watch::channel((channel, endpoint.into()));
        Self { rx }
    }
}

/// Adds `http://` when an endpoint has no URI scheme.
pub fn qualified(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// Builds a lazy channel from the qualified endpoint and returns both.
fn build_channel(endpoint: &str) -> Result<(Channel, String)> {
    let raw_endpoint = qualified(endpoint);
    let built = Endpoint::from_shared(raw_endpoint.clone())
        .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
    Ok((built.connect_lazy(), raw_endpoint))
}

/// Resolves a nonblank scheduler endpoint file from cluster configuration.
pub fn resolve_endpoint_file(cluster: &ClusterConfig) -> Option<PathBuf> {
    non_blank(&cluster.scheduler_endpoint_file).map(PathBuf::from)
}

fn non_blank(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn record_reload_metric(component: &'static str, result: &'static str) {
    metrics::counter!(
        SCHEDULER_ENDPOINT_RELOAD_METRIC,
        "component" => component,
        "result" => result
    )
    .increment(1);
}

/// File fingerprint and currently published endpoint held by the watcher.
struct WatcherState {
    /// File fingerprint after the first successful read.
    fingerprint: Option<(SystemTime, u64)>,
    endpoint: String,
}

async fn watch_file(
    path: PathBuf,
    interval: Duration,
    component: &'static str,
    tx: watch::Sender<(Channel, String)>,
) {
    let mut state = WatcherState {
        fingerprint: None,
        endpoint: tx.borrow().1.clone(),
    };

    // Check immediately before sleeping for the first interval.
    loop {
        check_and_publish(&path, component, &tx, &mut state);
        tokio::time::sleep(interval).await;
    }
}

/// Publishes a changed, valid endpoint while retaining the last good channel on failure.
fn check_and_publish(
    path: &std::path::Path,
    component: &'static str,
    tx: &watch::Sender<(Channel, String)>,
    state: &mut WatcherState,
) {
    // Skip rereading an unchanged file.
    let fingerprint = std::fs::metadata(path)
        .and_then(|meta| Ok((meta.modified()?, meta.len())))
        .ok();
    if let (Some(fingerprint), Some(held)) = (fingerprint, state.fingerprint) {
        if fingerprint == held {
            record_reload_metric(component, "unchanged");
            return;
        }
    }

    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let candidate = contents.trim();
            if candidate.is_empty() {
                warn!(
                    path = %path.display(),
                    component,
                    "scheduler endpoint file is empty; keeping the last endpoint that was read \
                     successfully. An empty file is not a valid target — write a real endpoint \
                     to change it, or stop mounting the file to fall back to the static endpoint \
                     at the next restart"
                );
                record_reload_metric(component, "empty_kept_previous");
                return;
            }

            // Build first so comparison uses the same qualified form as published state.
            match build_channel(candidate) {
                Ok((channel, qualified_candidate)) => {
                    if qualified_candidate == state.endpoint {
                        // Remember equivalent rewrites without publishing a duplicate channel.
                        state.fingerprint = fingerprint;
                        record_reload_metric(component, "same_value");
                        return;
                    }

                    info!(
                        component,
                        previous_endpoint = %state.endpoint,
                        new_endpoint = %qualified_candidate,
                        "scheduler endpoint target changed"
                    );
                    state.fingerprint = fingerprint;
                    state.endpoint = qualified_candidate.clone();
                    // No receivers means the owner is gone; the permanent watcher may continue.
                    let _ = tx.send((channel, qualified_candidate));
                    record_reload_metric(component, "switched");
                }
                Err(err) => {
                    // Invalid edits retain the last working endpoint and fingerprint.
                    error!(
                        path = %path.display(),
                        component,
                        candidate_endpoint = candidate,
                        error = %err,
                        "scheduler endpoint file names an endpoint that cannot be dialled; \
                         keeping the previous target"
                    );
                    record_reload_metric(component, "invalid_kept_previous");
                }
            }
        }
        Err(err) => {
            // Read errors retain the last successfully published endpoint.
            warn!(
                path = %path.display(),
                component,
                error = %err,
                "cannot read the scheduler endpoint file; keeping the last endpoint that was \
                 read successfully"
            );
            record_reload_metric(component, "error_kept_previous");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_config(endpoint_file: &str) -> ClusterConfig {
        ClusterConfig {
            scheduler_endpoint: None,
            scheduler_endpoint_file: endpoint_file.to_string(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            node_discovery_mode: Default::default(),
            kubernetes_discovery: Default::default(),
            static_discovery_nodes: Vec::new(),
            native_warmup_timeout_secs: 15,
            placement_shadow_k: crate::cfg::DEFAULT_PLACEMENT_SHADOW_K,
            node_registry_store: Default::default(),
        }
    }

    #[test]
    fn a_scheme_is_added_only_when_one_is_missing() {
        assert_eq!(qualified("scheduler:9090"), "http://scheduler:9090");
        assert_eq!(qualified("http://scheduler:9090"), "http://scheduler:9090");
        assert_eq!(
            qualified("https://scheduler:9090"),
            "https://scheduler:9090",
            "a configured TLS endpoint must not be downgraded to plaintext"
        );
    }

    #[test]
    fn resolve_endpoint_file_is_none_when_unset() {
        assert_eq!(resolve_endpoint_file(&cluster_config("")), None);
    }

    #[test]
    fn resolve_endpoint_file_treats_blank_as_absent() {
        assert_eq!(resolve_endpoint_file(&cluster_config("   ")), None);
    }

    #[test]
    fn resolve_endpoint_file_uses_the_cluster_field() {
        assert_eq!(
            resolve_endpoint_file(&cluster_config("  /etc/a  ")),
            Some(PathBuf::from("/etc/a"))
        );
    }

    #[tokio::test]
    async fn no_file_watch_never_changes_the_endpoint() {
        let source =
            SchedulerEndpointSource::spawn("http://scheduler-a:9090".to_string(), None, "test")
                .expect("a valid endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
    }

    #[tokio::test]
    async fn a_clone_observes_the_same_reloads_as_the_original() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090\n").expect("seed the file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");
        let clone = source.clone();

        wait_for(|| clone.current().1 == "http://scheduler-b:9090").await;
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if condition() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition was not met within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn adopts_a_new_endpoint_from_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090\n").expect("seed the file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;
    }

    #[tokio::test]
    async fn a_hot_reloaded_bare_host_port_is_qualified_before_it_is_published() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "scheduler-b:9090").expect("seed the file with a bare host:port");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;
    }

    #[tokio::test]
    async fn falls_back_to_the_static_endpoint_when_the_file_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source even if the file is absent");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "a file that was never read successfully must not blank the target"
        );
    }

    #[tokio::test]
    async fn ignores_an_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "").expect("write an empty file");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "an empty file is not a valid target and must not clear the endpoint"
        );
    }

    #[tokio::test]
    async fn ignores_a_file_naming_an_endpoint_that_cannot_be_dialled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "not a valid endpoint\twith control chars").expect("write junk");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path, Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            source.current().1,
            "http://scheduler-a:9090",
            "a candidate that fails to parse must keep the previous, working target"
        );
    }

    #[tokio::test]
    async fn picks_up_a_second_edit_after_the_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090").expect("first write");

        let source = SchedulerEndpointSource::spawn(
            "http://scheduler-a:9090".to_string(),
            Some((path.clone(), Duration::from_millis(20))),
            "test",
        )
        .expect("a valid static endpoint builds a source");
        wait_for(|| source.current().1 == "http://scheduler-b:9090").await;

        // Change length too so coarse filesystem mtimes cannot hide the edit.
        std::fs::write(&path, "http://scheduler-charlie:9090").expect("second write");
        wait_for(|| source.current().1 == "http://scheduler-charlie:9090").await;
    }

    #[tokio::test]
    async fn fixed_never_changes() {
        let channel = Endpoint::from_shared("http://fixed:9090".to_string())
            .expect("valid endpoint")
            .connect_lazy();
        let source = SchedulerEndpointSource::fixed(channel, "http://fixed:9090");
        assert_eq!(source.current().1, "http://fixed:9090");
        // Both accessors remain usable without a watcher.
        let _channel = source.channel();
        assert_eq!(source.current().1, "http://fixed:9090");
    }
}
