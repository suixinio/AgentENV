use std::cmp;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    interval: Duration,
}

pub struct ObservabilityReporter {
    config: ReporterConfig,
    service: Arc<ObservabilityService>,
    scheduler_channel: Channel,
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
        let scheduler_channel = Self::build_scheduler_channel(&config.scheduler_endpoint)?;

        Ok(Some(Self {
            config,
            service,
            scheduler_channel,
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
        let event_config = self.config.clone();
        let service = Arc::clone(&self.service);
        let event_service = Arc::clone(&self.service);
        let scheduler_channel = self.scheduler_channel.clone();
        let event_scheduler_channel = self.scheduler_channel.clone();
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

                match Self::send_heartbeat(
                    &config,
                    &service,
                    &scheduler_channel,
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
                            scheduler_endpoint = %config.scheduler_endpoint,
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
                        if let Err(err) = Self::send_sandbox_events(
                            &event_config,
                            &event_service,
                            &event_scheduler_channel,
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

    fn build_scheduler_channel(scheduler_endpoint: &str) -> Result<Channel> {
        let raw_endpoint = scheduler_endpoint.to_string();
        let endpoint = Endpoint::from_shared(raw_endpoint.clone())
            .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
        Ok(endpoint.connect_lazy())
    }

    async fn send_heartbeat(
        config: &ReporterConfig,
        service: &ObservabilityService,
        scheduler_channel: &Channel,
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
            scheduler_endpoint = %config.scheduler_endpoint,
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
        config: &ReporterConfig,
        service: &ObservabilityService,
        scheduler_channel: &Channel,
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
            scheduler_endpoint = %config.scheduler_endpoint,
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
        SchedulerClient::new(self.scheduler_channel.clone())
            .unregister_node(request)
            .await
            .context("unregister node rpc failed")?;
        Ok(())
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

        Some(ReporterConfig {
            scheduler_endpoint,
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

    fn make_cluster_config(endpoint: Option<&str>) -> ClusterConfig {
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
        ObservabilitySchedulerReportConfig {
            enabled: enabled.unwrap_or_default(),
            interval_secs: interval_secs.unwrap_or(5),
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
}
