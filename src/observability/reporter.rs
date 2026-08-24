use std::cmp;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{debug, error, info, trace, warn};

use super::ObservabilityService;
use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};
use crate::orchestrator::{SandboxLifecycleEvent, SandboxLifecycleEventType};
use crate::p2p::P2pEndpoint;
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};

const MAX_REPORT_BACKOFF: Duration = Duration::from_secs(60);
const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Metric name for [`SchedulerChannelSource::current`]'s file re-read
/// outcome. Mirrors `agentenv_api_control_plane_token_reload_total`
/// (`src/api/control_plane_gate.rs`) — same shape of problem, same shape of
/// answer.
const SCHEDULER_ENDPOINT_RELOAD_METRIC: &str =
    "agentenv_observability_scheduler_endpoint_reload_total";

/// Returned by [`ObservabilityReporter::send_heartbeat`] when the scheduler
/// rejects the heartbeat because this node's ID is not in its configured node
/// list. Detected in the reporter loop to emit an `error!`-level log with an
/// actionable remediation hint rather than a generic transient-failure warning.
#[derive(Debug, thiserror::Error)]
#[error("node is not in the scheduler's configured node list")]
struct HeartbeatNodeNotConfigured;

#[derive(Clone)]
struct ReporterConfig {
    scheduler_endpoint: String,
    /// A file re-read once per heartbeat/event tick that, once read
    /// successfully, overrides `scheduler_endpoint` — see
    /// [`ObservabilitySchedulerReportConfig::scheduler_endpoint_file`] for
    /// why this exists and why it is not a union with the static value.
    scheduler_endpoint_file: Option<PathBuf>,
    interval: Duration,
}

pub struct ObservabilityReporter {
    config: ReporterConfig,
    service: Arc<ObservabilityService>,
    channel_source: Arc<SchedulerChannelSource>,
    p2p_endpoint: Option<P2pEndpoint>,
    shutdown_tx: Option<watch::Sender<bool>>,
    heartbeat_join: Option<JoinHandle<()>>,
    event_join: Option<JoinHandle<()>>,
    /// Set to `true` on the first successful heartbeat RPC. Checked in
    /// [`shutdown`] to skip `UnregisterNode` when the reporter never managed
    /// to reach the scheduler at all.
    ever_heartbeat_succeeded: Arc<AtomicBool>,
}

impl ObservabilityReporter {
    pub fn new(
        service: Arc<ObservabilityService>,
        config: &ObservabilitySchedulerReportConfig,
        cluster_config: &ClusterConfig,
        p2p_endpoint: Option<P2pEndpoint>,
    ) -> Result<Option<Self>> {
        let Some(config) = ReporterConfig::resolve(config, cluster_config) else {
            return Ok(None);
        };
        let channel_source = Arc::new(SchedulerChannelSource::new(
            config.scheduler_endpoint.clone(),
            config.scheduler_endpoint_file.clone(),
        )?);

        Ok(Some(Self {
            config,
            service,
            channel_source,
            p2p_endpoint,
            shutdown_tx: None,
            heartbeat_join: None,
            event_join: None,
            ever_heartbeat_succeeded: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Spawns the background heartbeat task.
    ///
    /// [`ObservabilityReporter::new`] only builds the reporter without starting
    /// any background work.  Call `start` **exactly once** before calling
    /// [`shutdown`].
    pub fn start(&mut self) {
        if self.shutdown_tx.is_some() {
            warn!("reporter already started, ignoring duplicate start call");
            return;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = self.config.clone();
        let service = Arc::clone(&self.service);
        let event_service = Arc::clone(&self.service);
        let channel_source = Arc::clone(&self.channel_source);
        let event_channel_source = Arc::clone(&self.channel_source);
        let ever_heartbeat_succeeded = Arc::clone(&self.ever_heartbeat_succeeded);
        let p2p_endpoint = self.p2p_endpoint.clone();
        let mut heartbeat_shutdown_rx = shutdown_rx.clone();
        let mut event_shutdown_rx = shutdown_rx;
        let mut sandbox_event_rx = event_service.subscribe_sandbox_events();

        let heartbeat_join = tokio::spawn(async move {
            let mut backoff = config.interval;
            let mut wait = Duration::from_millis(100);
            let mut pending_cpu_config_json = service.take_cpu_config_json();

            loop {
                if wait > Duration::ZERO {
                    tokio::select! {
                        _ = sleep(wait) => {}
                        changed = heartbeat_shutdown_rx.changed() => {
                            if changed.is_err() || *heartbeat_shutdown_rx.borrow() {
                                info!("observability heartbeat reporter stopping");
                                return;
                            }
                        }
                    }
                }

                // Resolved fresh on every iteration, deliberately — not once
                // outside the loop. That is what makes a changed endpoint file
                // take effect on the *next* heartbeat rather than the next
                // restart: `current()` is where a hot-reloaded target actually
                // becomes traffic instead of merely a value that changed
                // somewhere.
                let (scheduler_channel, current_endpoint) = channel_source.current();

                match Self::send_heartbeat(
                    &service,
                    &scheduler_channel,
                    &current_endpoint,
                    &mut pending_cpu_config_json,
                    p2p_endpoint.as_ref(),
                )
                .await
                {
                    Ok(()) => {
                        ever_heartbeat_succeeded.store(true, Ordering::Relaxed);
                        backoff = config.interval;
                        wait = config.interval;
                    }
                    Err(ref err) if err.is::<HeartbeatNodeNotConfigured>() => {
                        error!(
                            node_id = %service.node_id(),
                            scheduler_endpoint = %current_endpoint,
                            retry_after_secs = backoff.as_secs(),
                            "scheduler rejected heartbeat: this node is not in the \
                             scheduler's configured node list — ensure \
                             AENV_NODE_ID matches a node name in the scheduler \
                             nodes configuration"
                        );
                        wait = backoff;
                        backoff = cmp::min(backoff.saturating_mul(2), MAX_REPORT_BACKOFF);
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            retry_after_secs = backoff.as_secs(),
                            "observability heartbeat failed"
                        );
                        wait = backoff;
                        backoff = cmp::min(backoff.saturating_mul(2), MAX_REPORT_BACKOFF);
                    }
                }
            }
        });

        let event_join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = event_shutdown_rx.changed() => {
                        if changed.is_err() || *event_shutdown_rx.borrow() {
                            info!("observability sandbox event reporter stopping");
                            return;
                        }
                    }
                    events = Self::recv_sandbox_event_batch(&mut sandbox_event_rx) => {
                        let Some(events) = events else {
                            return;
                        };
                        // Same reasoning as the heartbeat loop: fetched fresh
                        // for this batch, not cached across batches, so a
                        // hot-reloaded target applies to sandbox events too —
                        // both share `channel_source`, so they always agree on
                        // where "the scheduler" currently is.
                        let (event_scheduler_channel, event_endpoint) =
                            event_channel_source.current();
                        if let Err(err) = Self::send_sandbox_events(
                            &event_service,
                            &event_scheduler_channel,
                            &event_endpoint,
                            events,
                        ).await {
                            warn!(error = %err, "observability sandbox event batch report failed");
                        }
                    }
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.heartbeat_join = Some(heartbeat_join);
        self.event_join = Some(event_join);

        info!(
            scheduler_endpoint = %self.config.scheduler_endpoint,
            scheduler_endpoint_file = ?self.config.scheduler_endpoint_file.as_deref(),
            interval_secs = self.config.interval.as_secs(),
            "observability reporter started"
        );
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(join) = self.heartbeat_join.take() {
            if let Err(err) = join.await {
                warn!(error = %err, "observability heartbeat reporter task join failed");
            }
        }

        if let Some(join) = self.event_join.take() {
            if let Err(err) = join.await {
                warn!(error = %err, "observability sandbox event reporter task join failed");
            }
        }

        // If we never succeeded in sending a heartbeat, it's likely the scheduler
        // endpoint is misconfigured or the scheduler is unreachable. In that case,
        // skip the UnregisterNode RPC.
        if !self.ever_heartbeat_succeeded.load(Ordering::Relaxed) {
            debug!("skipping node unregister: no heartbeat ever succeeded");
            return Ok(());
        }

        for attempt in 1..=3 {
            match self.unregister_node().await {
                Ok(()) => {
                    info!(
                        node_id = %self.service.node_id(),
                        service_instance_id = %self.service.service_instance_id(),
                        attempt,
                        "observability node unregistered from scheduler"
                    );
                    return Ok(());
                }
                Err(err) => {
                    warn!(
                        node_id = %self.service.node_id(),
                        service_instance_id = %self.service.service_instance_id(),
                        attempt,
                        error = %err,
                        "failed to unregister node from scheduler during shutdown"
                    );
                    sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(
        service: &ObservabilityService,
        scheduler_channel: &Channel,
        scheduler_endpoint: &str,
        cpu_config_json: &mut Option<String>,
        p2p_endpoint: Option<&P2pEndpoint>,
    ) -> Result<()> {
        let mut snapshot = service
            .node_snapshot()
            .await
            .context("failed to collect heartbeat snapshot")?;
        snapshot.machine_info.cpu_config_json = cpu_config_json.clone();
        let node_id = snapshot.node_id.clone();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let req = Self::build_heartbeat_request(snapshot, now_ms, p2p_endpoint);

        let mut request = Request::new(req);
        request.set_timeout(GRPC_CALL_TIMEOUT);
        let response = SchedulerClient::new(scheduler_channel.clone())
            .heartbeat(request)
            .await
            .map_err(|s| {
                if s.code() == tonic::Code::InvalidArgument
                    && s.message().contains("node is not in scheduler node list")
                {
                    anyhow::Error::new(HeartbeatNodeNotConfigured)
                } else {
                    anyhow::Error::from(s).context("heartbeat rpc failed")
                }
            })?
            .into_inner();

        *cpu_config_json = None;

        if !response.cpu_config_json.is_empty() {
            service.store_cluster_cpu_config(response.cpu_config_json);
            info!("received cluster cpu config intersection from scheduler");
        }

        trace!(
            node_id = %node_id,
            scheduler_endpoint = %scheduler_endpoint,
            "observability heartbeat sent"
        );

        Ok(())
    }

    async fn recv_sandbox_event_batch(
        rx: &mut broadcast::Receiver<SandboxLifecycleEvent>,
    ) -> Option<Vec<SandboxLifecycleEvent>> {
        let first = loop {
            match rx.recv().await {
                Ok(event) => break event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "observability sandbox event receiver lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("observability sandbox event channel closed");
                    return None;
                }
            }
        };

        let mut events = Vec::with_capacity(rx.len() + 1);
        events.push(first);
        loop {
            match rx.try_recv() {
                Ok(event) => events.push(event),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    warn!(skipped, "observability sandbox event receiver lagged");
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    debug!("observability sandbox event channel closed");
                    break;
                }
            }
        }

        Some(events)
    }

    async fn send_sandbox_events(
        service: &ObservabilityService,
        scheduler_channel: &Channel,
        scheduler_endpoint: &str,
        events: Vec<SandboxLifecycleEvent>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let event_count = events.len();
        let mut request = Request::new(Self::build_sandbox_event_request(service, events));
        request.set_timeout(GRPC_CALL_TIMEOUT);
        SchedulerClient::new(scheduler_channel.clone())
            .report_sandbox_event(request)
            .await
            .context("sandbox event batch report rpc failed")?;

        trace!(
            node_id = %service.node_id(),
            event_count,
            scheduler_endpoint = %scheduler_endpoint,
            "observability sandbox event batch sent"
        );

        Ok(())
    }

    // `sandbox_ids` is deprecated on the wire and still sent on purpose: the
    // controller deletes every binding a node owns when it receives an empty
    // roster, so a node that stopped sending the old field before the whole
    // fleet reads the new one would have its sandboxes answer 404 on the data
    // plane for the length of the rolling window. Both fields travel until the
    // controller reports it has seen no legacy roster.
    #[allow(deprecated)]
    fn build_heartbeat_request(
        snapshot: super::NodeSnapshot,
        now_ms: i64,
        p2p_endpoint: Option<&P2pEndpoint>,
    ) -> scheduler::HeartbeatRequest {
        scheduler::HeartbeatRequest {
            node_id: snapshot.node_id,
            cluster_id: snapshot.cluster_id.to_string(),
            service_instance_id: snapshot.service_instance_id,
            version: snapshot.version,
            commit: snapshot.commit,
            machine_info: Some(scheduler::MachineInfo {
                cpu_family: snapshot.machine_info.cpu_family,
                cpu_model: snapshot.machine_info.cpu_model,
                cpu_model_name: snapshot.machine_info.cpu_model_name,
                cpu_architecture: snapshot.machine_info.cpu_architecture,
                cpu_config_json: snapshot.machine_info.cpu_config_json.unwrap_or_default(),
            }),
            snapshot: Some(scheduler::NodeSnapshot {
                // An isolated node keeps heartbeating — it is healthy and still
                // serving sandboxes — and says so here, which is what takes it
                // out of scheduling without taking it out of the cluster.
                status: if snapshot.draining {
                    scheduler::NodeStatus::Draining.into()
                } else {
                    scheduler::NodeStatus::Ready.into()
                },
                allocated_cpu: snapshot.metrics.allocated_cpu,
                allocated_memory_bytes: snapshot.metrics.allocated_memory_bytes,
                cpu_percent: snapshot.metrics.cpu_percent,
                cpu_count: snapshot.metrics.cpu_count,
                memory_used_bytes: snapshot.metrics.memory_used_bytes,
                memory_total_bytes: snapshot.metrics.memory_total_bytes,
                disks: snapshot
                    .metrics
                    .disks
                    .into_iter()
                    .map(|disk| scheduler::DiskMetric {
                        mount_point: disk.mount_point,
                        device: disk.device,
                        filesystem_type: disk.filesystem_type,
                        used_bytes: disk.used_bytes,
                        total_bytes: disk.total_bytes,
                    })
                    .collect(),
                sandbox_count: snapshot.sandbox_count,
                sandbox_starting_count: snapshot.sandbox_starting_count,
                create_successes: snapshot.create_successes,
                create_fails: snapshot.create_fails,
                reported_at_unix_ms: now_ms,
                paused_sandbox_count: snapshot.paused_sandbox_count,
                paused_allocated_cpu: snapshot.metrics.paused_allocated_cpu,
                paused_allocated_memory_bytes: snapshot.metrics.paused_allocated_memory_bytes,
            }),
            sandbox_ids: snapshot
                .sandbox_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            p2p_endpoint: p2p_endpoint.map(|endpoint| scheduler::P2pEndpoint {
                backend: endpoint.backend.clone(),
                address: endpoint.address.clone(),
            }),
            roster: snapshot
                .sandbox_roster
                .into_iter()
                .map(|entry| scheduler::SandboxRosterEntry {
                    sandbox_id: entry.sandbox_id.to_string(),
                    execution_id: entry.execution_id.to_string(),
                    projection_ttl_secs: entry.projection_ttl_secs,
                })
                .collect(),
        }
    }

    fn build_sandbox_event_request(
        service: &ObservabilityService,
        events: Vec<SandboxLifecycleEvent>,
    ) -> scheduler::ReportSandboxEventRequest {
        scheduler::ReportSandboxEventRequest {
            node_id: service.node_id().to_string(),
            cluster_id: service.cluster_id().to_string(),
            service_instance_id: service.service_instance_id().to_string(),
            events: events.into_iter().map(Self::map_sandbox_event).collect(),
        }
    }

    fn map_sandbox_event(event: SandboxLifecycleEvent) -> scheduler::SandboxEvent {
        scheduler::SandboxEvent {
            sandbox_id: event.sandbox_id.to_string(),
            event_type: Self::map_sandbox_event_type(event.event_type).into(),
            // The guard the receiver deletes a projection under. An empty value
            // there means "no guard at all", and the receiver then declines to
            // delete rather than deleting unguarded — so an event that goes out
            // without this is an event that does nothing.
            execution_id: event.execution_id.to_string(),
            requested_cpu: event.resources.cpu_count,
            requested_memory_bytes: u64::from(event.resources.memory_mib) * 1024 * 1024,
            requested_disk_bytes: u64::from(event.resources.disk_size_mib) * 1024 * 1024,
        }
    }

    fn map_sandbox_event_type(
        event_type: SandboxLifecycleEventType,
    ) -> scheduler::SandboxEventType {
        match event_type {
            SandboxLifecycleEventType::Create => scheduler::SandboxEventType::Create,
            SandboxLifecycleEventType::Delete => scheduler::SandboxEventType::Delete,
            SandboxLifecycleEventType::Pause => scheduler::SandboxEventType::Pause,
            SandboxLifecycleEventType::Resume => scheduler::SandboxEventType::Resume,
            SandboxLifecycleEventType::Fork => scheduler::SandboxEventType::Fork,
        }
    }

    async fn unregister_node(&self) -> Result<()> {
        let mut request = Request::new(scheduler::UnregisterNodeRequest {
            node_id: self.service.node_id().to_string(),
            service_instance_id: self.service.service_instance_id().to_string(),
        });
        request.set_timeout(GRPC_CALL_TIMEOUT);
        // Whichever scheduler this reporter is currently heartbeating, not
        // necessarily the one it started on — a node that switched targets
        // mid-life should unregister itself from the one that actually holds
        // its binding.
        let (channel, _endpoint) = self.channel_source.current();
        SchedulerClient::new(channel)
            .unregister_node(request)
            .await
            .context("unregister node rpc failed")?;
        Ok(())
    }
}

/// Where the reporter dials the scheduler right now, and how that can change
/// while the process runs.
///
/// Two sources: [`ReporterConfig::scheduler_endpoint`], fixed for the life of
/// the process, and an optional file
/// ([`ReporterConfig::scheduler_endpoint_file`]) re-read once per
/// heartbeat/event tick by [`current`](Self::current). They do **not**
/// union — a heartbeat can only go to one place — so a file that has been
/// read successfully at least once overrides the static value outright,
/// rather than adding to it the way `ControlPlaneGate`'s two credential
/// sources do (`src/api/control_plane_gate.rs`).
///
/// 🔴 When no file is configured, [`current`](Self::current) never touches
/// the filesystem: it hands out clones of the one channel built at
/// construction, forever — exactly what the reporter did before this existed.
/// That is what keeps every deployment that has not opted into
/// `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE` byte-for-byte unchanged.
struct SchedulerChannelSource {
    /// `None` when no file is configured — the off position, and the one
    /// every deployment ships in today.
    file: Option<PathBuf>,
    state: Mutex<ChannelSourceState>,
}

struct ChannelSourceState {
    /// Modification time and length of the file content backing `endpoint` /
    /// `channel`. `None` until the file has been read successfully at least
    /// once; used to skip re-parsing a file that stat says has not changed.
    fingerprint: Option<(SystemTime, u64)>,
    /// The endpoint currently backing `channel`. Starts as the static value
    /// and is only ever replaced by a *successful* file read that also built
    /// a working channel — see [`SchedulerChannelSource::current`].
    endpoint: String,
    channel: Channel,
}

impl SchedulerChannelSource {
    /// Builds the initial channel from the static endpoint only — the file,
    /// if any, is first consulted by the loop's first call to
    /// [`current`](Self::current), not here. A file that is misconfigured or
    /// not yet mounted must never fail process startup; only a bad *static*
    /// endpoint does, which is unchanged from before this type existed.
    fn new(static_endpoint: String, file: Option<PathBuf>) -> Result<Self> {
        let channel = Self::build_channel(&static_endpoint)?;
        Ok(Self {
            file,
            state: Mutex::new(ChannelSourceState {
                fingerprint: None,
                endpoint: static_endpoint,
                channel,
            }),
        })
    }

    fn build_channel(endpoint: &str) -> Result<Channel> {
        let raw_endpoint = endpoint.to_string();
        let built = Endpoint::from_shared(raw_endpoint.clone())
            .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
        Ok(built.connect_lazy())
    }

    /// The channel and endpoint to send the next RPC on.
    ///
    /// Infallible by design: any problem reading, parsing, or dialling a
    /// candidate from the file is logged and metered, and the channel already
    /// in force is returned unchanged. The file mechanism can only ever hand
    /// out a channel it built successfully — it can never hand back "no
    /// channel", and it can never leave a caller with a channel that was
    /// discarded mid-swap. Building the replacement happens under the same
    /// lock that publishes it, so a heartbeat and a sandbox-event batch
    /// racing this at the same moment either both see the old target or both
    /// see the new one, never a mix, and an in-flight RPC on the outgoing
    /// channel is unaffected — it already holds its own clone and keeps
    /// running to completion or failure on it; nothing here cancels it.
    fn current(&self) -> (Channel, String) {
        let Some(path) = self.file.as_ref() else {
            let state = self
                .state
                .lock()
                .expect("scheduler channel state is poisoned");
            return (state.channel.clone(), state.endpoint.clone());
        };

        let mut state = self
            .state
            .lock()
            .expect("scheduler channel state is poisoned");

        // Skip the read when the file is byte-for-byte the one already held.
        // A heartbeat is rare enough that a stat per tick costs nothing, and
        // this keeps the common case — nobody has touched the file — to
        // exactly that.
        let fingerprint = std::fs::metadata(path)
            .and_then(|meta| Ok((meta.modified()?, meta.len())))
            .ok();
        if let (Some(fingerprint), Some(held)) = (fingerprint, state.fingerprint) {
            if fingerprint == held {
                metrics::counter!(SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "unchanged")
                    .increment(1);
                return (state.channel.clone(), state.endpoint.clone());
            }
        }

        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let candidate = contents.trim();
                if candidate.is_empty() {
                    warn!(
                        path = %path.display(),
                        "scheduler endpoint file is empty; keeping the last endpoint that was \
                         read successfully. An empty file is not a valid target — write a real \
                         endpoint to change it, or stop mounting the file to fall back to the \
                         static endpoint at the next restart"
                    );
                    metrics::counter!(
                        SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "empty_kept_previous"
                    )
                    .increment(1);
                    return (state.channel.clone(), state.endpoint.clone());
                }

                if candidate == state.endpoint {
                    // Same target, different bytes on disk (e.g. a rewrite
                    // with identical content, or added trailing whitespace).
                    // Remember the new fingerprint so the next tick takes the
                    // fast path above, but there is no channel to rebuild.
                    state.fingerprint = fingerprint;
                    metrics::counter!(
                        SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "same_value"
                    )
                    .increment(1);
                    return (state.channel.clone(), state.endpoint.clone());
                }

                match Self::build_channel(candidate) {
                    Ok(channel) => {
                        info!(
                            previous_endpoint = %state.endpoint,
                            new_endpoint = %candidate,
                            "observability heartbeat target changed"
                        );
                        state.fingerprint = fingerprint;
                        state.endpoint = candidate.to_string();
                        state.channel = channel.clone();
                        metrics::counter!(
                            SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "switched"
                        )
                        .increment(1);
                        (channel, candidate.to_string())
                    }
                    Err(err) => {
                        // 🔴 Never fail open, and never fail *closed* either —
                        // a malformed edit to the ConfigMap must not stop
                        // heartbeating. Keep dialling the last endpoint that
                        // parsed, and do not update the fingerprint: an
                        // operator fixing the typo produces a new mtime/len,
                        // which is picked up on the very next tick without
                        // needing this branch to remember anything.
                        error!(
                            path = %path.display(),
                            candidate_endpoint = candidate,
                            error = %err,
                            "scheduler endpoint file names an endpoint that cannot be dialled; \
                             keeping the previous heartbeat target"
                        );
                        metrics::counter!(
                            SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "invalid_kept_previous"
                        )
                        .increment(1);
                        (state.channel.clone(), state.endpoint.clone())
                    }
                }
            }
            Err(err) => {
                // 🔴 Same "never fail open" rule as
                // `ControlPlaneGate::file_tokens`: a read error is not
                // evidence the endpoint changed, it is evidence of nothing at
                // all, and treating it as "fall back to the static value"
                // would let a single disk hiccup silently redirect every
                // future heartbeat.
                warn!(
                    path = %path.display(),
                    error = %err,
                    "cannot read the scheduler endpoint file; keeping the last endpoint that was \
                     read successfully"
                );
                metrics::counter!(
                    SCHEDULER_ENDPOINT_RELOAD_METRIC, "result" => "error_kept_previous"
                )
                .increment(1);
                (state.channel.clone(), state.endpoint.clone())
            }
        }
    }
}

impl ReporterConfig {
    fn resolve(
        config: &ObservabilitySchedulerReportConfig,
        cluster_config: &ClusterConfig,
    ) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let Some(scheduler_endpoint) = cluster_config
            .scheduler_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned)
        else {
            warn!(
                "observability scheduler reporter is enabled but cluster scheduler endpoint is not configured"
            );
            return None;
        };

        let scheduler_endpoint_file = Some(config.scheduler_endpoint_file.trim())
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        Some(ReporterConfig {
            scheduler_endpoint,
            scheduler_endpoint_file,
            interval: Duration::from_secs(config.interval_secs.max(1)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};
    use crate::observability::{MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
    use crate::orchestrator::SandboxRosterEntry;
    use crate::types::{ExecutionId, SandboxId, SandboxResources};

    pub(super) fn make_cluster_config(endpoint: Option<&str>) -> ClusterConfig {
        ClusterConfig {
            scheduler_endpoint: endpoint.map(|s| s.to_string()),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
        }
    }

    fn make_report_config(
        enabled: Option<bool>,
        interval_secs: Option<u64>,
    ) -> ObservabilitySchedulerReportConfig {
        make_report_config_with_file(enabled, interval_secs, None)
    }

    pub(super) fn make_report_config_with_file(
        enabled: Option<bool>,
        interval_secs: Option<u64>,
        scheduler_endpoint_file: Option<&str>,
    ) -> ObservabilitySchedulerReportConfig {
        ObservabilitySchedulerReportConfig {
            enabled: enabled.unwrap_or_default(),
            interval_secs: interval_secs.unwrap_or(5),
            scheduler_endpoint_file: scheduler_endpoint_file.unwrap_or_default().to_string(),
        }
    }

    #[test]
    fn test_resolve_returns_none_when_no_config() {
        let cfg = make_report_config(None, None);
        let cluster = make_cluster_config(None);
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_returns_none_when_report_is_disabled() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(false), Some(10));
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_returns_none_when_endpoint_is_blank() {
        let cluster = make_cluster_config(Some("   "));
        let cfg = make_report_config(Some(true), None);
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_uses_config_values() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(true), Some(10));
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.scheduler_endpoint, "http://scheduler:9090");
        assert_eq!(result.interval, Duration::from_secs(10));
    }

    #[test]
    fn test_resolve_clamps_interval_to_minimum_one() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(true), Some(0));
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.interval, Duration::from_secs(1));
    }

    fn node_snapshot(roster: Vec<SandboxRosterEntry>) -> NodeSnapshot {
        NodeSnapshot {
            version: "test".to_string(),
            commit: "test".to_string(),
            node_id: "node-a".to_string(),
            service_instance_id: "instance-a".to_string(),
            cluster_id: uuid::Uuid::nil(),
            machine_info: MachineInfo {
                cpu_family: String::new(),
                cpu_model: String::new(),
                cpu_model_name: String::new(),
                cpu_architecture: String::new(),
                cpu_config_json: None,
            },
            sandbox_count: roster.len() as u32,
            sandbox_ids: roster.iter().map(|entry| entry.sandbox_id).collect(),
            sandbox_roster: roster,
            metrics: NodeMetricsSnapshot {
                allocated_cpu: 0,
                allocated_memory_bytes: 0,
                cpu_percent: 0,
                cpu_count: 0,
                memory_used_bytes: 0,
                memory_total_bytes: 0,
                disks: Vec::new(),
                paused_allocated_cpu: 0,
                paused_allocated_memory_bytes: 0,
            },
            draining: false,
            create_successes: 0,
            create_fails: 0,
            sandbox_starting_count: 0,
            paused_sandbox_count: 0,
        }
    }

    /// The heartbeat is the repair path for a projection write that was lost,
    /// so it has to carry the sandbox's own budget. A repair that installs the
    /// receiver's default instead turns one dropped write into a permanently
    /// short-lived record — and nothing anywhere reports that it happened.
    #[test]
    fn the_heartbeat_roster_carries_each_sandbox_budget() {
        let entry = SandboxRosterEntry {
            sandbox_id: SandboxId::new(),
            execution_id: ExecutionId::new(),
            projection_ttl_secs: 86_460,
        };

        let request =
            ObservabilityReporter::build_heartbeat_request(node_snapshot(vec![entry]), 0, None);

        assert_eq!(request.roster.len(), 1);
        assert_eq!(request.roster[0].sandbox_id, entry.sandbox_id.to_string());
        assert_eq!(
            request.roster[0].execution_id,
            entry.execution_id.to_string()
        );
        assert_eq!(request.roster[0].projection_ttl_secs, 86_460);
    }

    /// 🔴 The control face: a node with no ceiling says 0, and 0 is the value
    /// the receiver reads as "use your own default". It is never "do not
    /// expire" — a record that outlives every path able to delete it is a route
    /// pointing at a sandbox nobody can reach.
    #[test]
    fn a_roster_entry_without_a_budget_says_zero() {
        let entry = SandboxRosterEntry {
            sandbox_id: SandboxId::new(),
            execution_id: ExecutionId::new(),
            projection_ttl_secs: 0,
        };

        let request =
            ObservabilityReporter::build_heartbeat_request(node_snapshot(vec![entry]), 0, None);

        assert_eq!(request.roster[0].projection_ttl_secs, 0);
    }

    /// Every lifecycle event names the run it belongs to. The receiver guards a
    /// projection delete with this value and declines to delete when it is
    /// missing, so an event that goes out without one is an event that does
    /// nothing at all.
    #[test]
    fn every_lifecycle_event_names_its_incarnation() {
        for event_type in [
            SandboxLifecycleEventType::Create,
            SandboxLifecycleEventType::Delete,
            SandboxLifecycleEventType::Pause,
            SandboxLifecycleEventType::Resume,
            SandboxLifecycleEventType::Fork,
        ] {
            let event = SandboxLifecycleEvent {
                event_type,
                sandbox_id: SandboxId::new(),
                execution_id: ExecutionId::new(),
                resources: SandboxResources {
                    cpu_count: 2,
                    memory_mib: 512,
                    disk_size_mib: 1024,
                },
            };

            let wire = ObservabilityReporter::map_sandbox_event(event);

            assert_eq!(wire.sandbox_id, event.sandbox_id.to_string());
            assert_eq!(
                wire.execution_id,
                event.execution_id.to_string(),
                "{event_type:?} must name the run it belongs to"
            );
            assert!(!wire.execution_id.is_empty());
            assert_eq!(wire.requested_memory_bytes, 512 * 1024 * 1024);
            assert_eq!(wire.requested_disk_bytes, 1024 * 1024 * 1024);
        }
    }

    #[test]
    fn test_heartbeat_node_not_configured_is_detectable_via_anyhow() {
        let err = anyhow::Error::new(HeartbeatNodeNotConfigured);
        assert!(
            err.is::<HeartbeatNodeNotConfigured>(),
            "anyhow::Error::is should detect HeartbeatNodeNotConfigured"
        );
    }

    #[test]
    fn test_heartbeat_node_not_configured_displays_message() {
        let msg = HeartbeatNodeNotConfigured.to_string();
        assert!(
            msg.contains("node is not in"),
            "error message should be descriptive, got: {msg}"
        );
    }

    #[test]
    fn test_regular_anyhow_error_is_not_heartbeat_node_not_configured() {
        let err = anyhow::anyhow!("some transient network error");
        assert!(
            !err.is::<HeartbeatNodeNotConfigured>(),
            "generic errors must not be mistaken for HeartbeatNodeNotConfigured"
        );
    }

    #[test]
    fn resolve_leaves_the_file_unset_by_default() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(true), None);
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(
            result.scheduler_endpoint_file, None,
            "no deployment has opted in yet; this must stay off by default"
        );
    }

    #[test]
    fn resolve_trims_and_picks_up_a_configured_endpoint_file() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config_with_file(
            Some(true),
            None,
            Some("  /etc/agentenv/heartbeat/scheduler-endpoint  "),
        );
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(
            result.scheduler_endpoint_file,
            Some(PathBuf::from("/etc/agentenv/heartbeat/scheduler-endpoint"))
        );
    }

    #[test]
    fn resolve_treats_a_blank_endpoint_file_as_unset() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config_with_file(Some(true), None, Some("   "));
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.scheduler_endpoint_file, None);
    }

    /// The behaviour this whole slice exists to add: a channel source with no
    /// file configured is exactly what the reporter did before it, forever —
    /// no stat, no re-read, no drift from the static endpoint.
    #[tokio::test]
    async fn channel_source_with_no_file_never_changes_its_endpoint() {
        let source = SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), None)
            .expect("a valid endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
        // A second call must agree, and must not have gone looking for a file
        // that was never configured.
        let (_channel, endpoint) = source.current();
        assert_eq!(endpoint, "http://scheduler-a:9090");
    }

    #[tokio::test]
    async fn channel_source_adopts_a_new_endpoint_from_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090\n").expect("seed the file");

        let source = SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), Some(path))
            .expect("a valid static endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(
            endpoint, "http://scheduler-b:9090",
            "a readable file must override the static endpoint"
        );
    }

    #[tokio::test]
    async fn channel_source_falls_back_to_the_static_endpoint_when_the_file_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist");

        let source = SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), Some(path))
            .expect("a valid static endpoint builds a source even if the file is absent");

        let (_channel, endpoint) = source.current();
        assert_eq!(
            endpoint, "http://scheduler-a:9090",
            "a file that was never read successfully must not blank the target"
        );
    }

    #[tokio::test]
    async fn channel_source_ignores_an_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "").expect("write an empty file");

        let source = SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), Some(path))
            .expect("a valid static endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(
            endpoint, "http://scheduler-a:9090",
            "an empty file is not a valid target and must not clear the endpoint"
        );
    }

    #[tokio::test]
    async fn channel_source_ignores_a_file_naming_an_endpoint_that_cannot_be_dialled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "not a valid endpoint\twith control chars").expect("write junk");

        let source = SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), Some(path))
            .expect("a valid static endpoint builds a source");

        let (_channel, endpoint) = source.current();
        assert_eq!(
            endpoint, "http://scheduler-a:9090",
            "a candidate that fails to parse must keep the previous, working target"
        );
    }

    #[tokio::test]
    async fn channel_source_picks_up_a_second_edit_after_the_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("scheduler-endpoint");
        std::fs::write(&path, "http://scheduler-b:9090").expect("first write");

        let source =
            SchedulerChannelSource::new("http://scheduler-a:9090".to_string(), Some(path.clone()))
                .expect("a valid static endpoint builds a source");
        assert_eq!(source.current().1, "http://scheduler-b:9090");

        // A different length as well as different bytes: the fingerprint this
        // relies on is (mtime, len), and two writes issued back to back on a
        // filesystem with coarse mtime resolution could otherwise collide on
        // both — this makes the length alone enough to tell them apart even
        // if that ever happens.
        std::thread::sleep(Duration::from_millis(5));
        std::fs::write(&path, "http://scheduler-charlie:9090").expect("second write");
        assert_eq!(
            source.current().1,
            "http://scheduler-charlie:9090",
            "the source must keep tracking the file across more than one edit"
        );
    }
}

/// The one test in this file that goes over a real socket rather than
/// inspecting [`SchedulerChannelSource`]'s state directly.
///
/// 🔴 It exists because of what a real regression on this repository's dev
/// cluster looked like: a config-reload change that read the new value fine,
/// updated every place a human would check, passed every test that asserted
/// on *values* — and never dialled anywhere else, because nothing rebuilt the
/// channel. Every test above this one would pass under that bug, because they
/// all call [`SchedulerChannelSource::current`] directly and that function
/// was exactly what regressed. This test instead runs the reporter's real
/// background loop against two real gRPC servers and asks the only question
/// that matters: after the switch, which one receives the next heartbeat.
#[cfg(test)]
mod against_a_scheduler {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration as StdDuration, Instant};

    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    use super::tests::{make_cluster_config, make_report_config_with_file};
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{Orchestrator, SandboxOrchestration};
    use crate::proto::scheduler::scheduler_server::{Scheduler, SchedulerServer};

    /// A scheduler that does nothing but answer `Heartbeat` and
    /// `UnregisterNode`, counting the heartbeats it received. Enough to tell
    /// "traffic reached this address" from "traffic did not" — which is the
    /// only thing this test is about.
    #[derive(Default)]
    struct CountingScheduler {
        heartbeats: AtomicUsize,
    }

    impl CountingScheduler {
        fn heartbeat_count(&self) -> usize {
            self.heartbeats.load(AtomicOrdering::SeqCst)
        }
    }

    #[tonic::async_trait]
    impl Scheduler for Arc<CountingScheduler> {
        async fn heartbeat(
            &self,
            _request: Request<scheduler::HeartbeatRequest>,
        ) -> Result<Response<scheduler::HeartbeatResponse>, Status> {
            self.heartbeats.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Response::new(scheduler::HeartbeatResponse::default()))
        }
        async fn unregister_node(
            &self,
            _request: Request<scheduler::UnregisterNodeRequest>,
        ) -> Result<Response<scheduler::UnregisterNodeResponse>, Status> {
            Ok(Response::new(scheduler::UnregisterNodeResponse::default()))
        }
        async fn record_assignment(
            &self,
            _request: Request<scheduler::RecordAssignmentRequest>,
        ) -> Result<Response<scheduler::RecordAssignmentResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn lookup_node(
            &self,
            _request: Request<scheduler::LookupNodeRequest>,
        ) -> Result<Response<scheduler::LookupNodeResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn get_node(
            &self,
            _request: Request<scheduler::GetNodeRequest>,
        ) -> Result<Response<scheduler::GetNodeResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn schedule(
            &self,
            _request: Request<scheduler::ScheduleRequest>,
        ) -> Result<Response<scheduler::ScheduleResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn list_nodes(
            &self,
            _request: Request<scheduler::ListNodesRequest>,
        ) -> Result<Response<scheduler::ListNodesResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn report_sandbox_event(
            &self,
            _request: Request<scheduler::ReportSandboxEventRequest>,
        ) -> Result<Response<scheduler::ReportSandboxEventResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn list_observed_nodes(
            &self,
            _request: Request<scheduler::ListObservedNodesRequest>,
        ) -> Result<Response<scheduler::ListObservedNodesResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn list_p2p_peers(
            &self,
            _request: Request<scheduler::ListP2pPeersRequest>,
        ) -> Result<Response<scheduler::ListP2pPeersResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn record_p2p_artifact(
            &self,
            _request: Request<scheduler::RecordP2pArtifactRequest>,
        ) -> Result<Response<scheduler::RecordP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn forget_p2p_artifact(
            &self,
            _request: Request<scheduler::ForgetP2pArtifactRequest>,
        ) -> Result<Response<scheduler::ForgetP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn lookup_p2p_artifact(
            &self,
            _request: Request<scheduler::LookupP2pArtifactRequest>,
        ) -> Result<Response<scheduler::LookupP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
        async fn list_registry_sandboxes(
            &self,
            _request: Request<scheduler::ListRegistrySandboxesRequest>,
        ) -> Result<Response<scheduler::ListRegistrySandboxesResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
    }

    /// A counting scheduler on a real port, serving until its shutdown sender
    /// is dropped or fired.
    async fn scheduler_on_a_socket() -> (Arc<CountingScheduler>, SocketAddr, oneshot::Sender<()>) {
        let scheduler = Arc::new(CountingScheduler::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("the bound address");
        let (tx, rx) = oneshot::channel();

        let served = Arc::clone(&scheduler);
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(served))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }

        (scheduler, addr, tx)
    }

    /// A cheap, real `ObservabilityService` — an in-memory orchestrator with
    /// nothing in it. `node_snapshot()` over an empty orchestrator is exactly
    /// as real as one holding sandboxes; this test is about where the
    /// heartbeat goes, not what is in it.
    async fn test_service() -> Arc<ObservabilityService> {
        let orchestrator = Orchestrator::with_in_memory_store().await;
        let orchestration: Arc<dyn SandboxOrchestration> = orchestrator as _;
        Arc::new(
            ObservabilityService::new(
                NodeIdentity {
                    id: "node-under-test".to_string(),
                    cluster_id: uuid::Uuid::nil(),
                    service_instance_id: "instance-under-test".to_string(),
                    commit: "test".to_string(),
                    version: "test".to_string(),
                },
                orchestration,
                None,
                Arc::new(std::sync::RwLock::new(None)),
            )
            .await,
        )
    }

    async fn wait_until(timeout: StdDuration, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        loop {
            if condition() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "condition not met within {timeout:?}"
            );
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
    }

    /// T-P4-0. The reporter is pointed at scheduler A; the endpoint file is
    /// rewritten to name scheduler B while the reporter keeps running; the
    /// very next heartbeat must land on B, with no restart.
    ///
    /// This is the test the task's two required mutations must turn red:
    /// dropping the channel rebuild, or dropping the change detection
    /// entirely, both leave every heartbeat going to A forever, and this test
    /// times out waiting for B to see one.
    #[tokio::test]
    async fn a_hot_reloaded_endpoint_actually_moves_the_traffic() {
        let (scheduler_a, addr_a, _shutdown_a) = scheduler_on_a_socket().await;
        let (scheduler_b, addr_b, _shutdown_b) = scheduler_on_a_socket().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("scheduler-endpoint");
        std::fs::write(&file_path, format!("http://{addr_a}")).expect("seed the file with A");

        let service = test_service().await;
        let cluster = make_cluster_config(Some(&format!("http://{addr_a}")));
        let report_config = make_report_config_with_file(
            Some(true),
            Some(1),
            Some(file_path.to_str().expect("temp paths are valid utf-8")),
        );

        let mut reporter = ObservabilityReporter::new(service, &report_config, &cluster, None)
            .expect("a valid endpoint builds a reporter")
            .expect("reporting is enabled and the endpoint is non-empty");
        reporter.start();

        wait_until(StdDuration::from_secs(5), || {
            scheduler_a.heartbeat_count() >= 1
        })
        .await;
        assert_eq!(
            scheduler_b.heartbeat_count(),
            0,
            "nothing has told the reporter about B yet"
        );

        // The hot-reload moment: rewrite the file, touch nothing else. No
        // restart, no reconstruction of the reporter.
        std::fs::write(&file_path, format!("http://{addr_b}")).expect("rewrite the file to B");

        wait_until(StdDuration::from_secs(10), || {
            scheduler_b.heartbeat_count() >= 1
        })
        .await;

        reporter
            .shutdown()
            .await
            .expect("both schedulers answer UnregisterNode");
    }
}
