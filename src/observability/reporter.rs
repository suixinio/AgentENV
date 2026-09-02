use std::cmp;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{debug, error, info, trace, warn};

use super::ObservabilityService;
use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};
use crate::orchestrator::{SandboxLifecycleEvent, SandboxLifecycleEventType};
use crate::p2p::P2pEndpoint;
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};
use crate::scheduler_endpoint::SchedulerEndpointSource;

const MAX_REPORT_BACKOFF: Duration = Duration::from_secs(60);
const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Returned by [`ObservabilityReporter::send_heartbeat`] when the scheduler
/// rejects the heartbeat because this node's ID is not in its configured node
/// list. Detected in the reporter loop to emit an `error!`-level log with an
/// actionable remediation hint rather than a generic transient-failure warning.
#[derive(Debug, thiserror::Error)]
#[error("node is not in the scheduler's configured node list")]
struct HeartbeatNodeNotConfigured;

// Sandboxes the control plane named as no longer this node's, carried between
// heartbeats so a naming has to survive one.
//
// A pause registers its row only after the node has already begun reporting the
// sandbox paused, and one heartbeat can land in that window. Requiring two
// consecutive namings puts a whole interval between them, which is longer than
// that window, and costs one extra interval on every real discard.
#[derive(Default)]
struct DisownedCandidates(std::collections::HashSet<crate::types::SandboxId>);

impl DisownedCandidates {
    // Replaces the candidates with `named` and returns those named last time too.
    fn confirm(&mut self, named: Vec<crate::types::SandboxId>) -> Vec<crate::types::SandboxId> {
        let repeated: Vec<_> = named
            .iter()
            .copied()
            .filter(|sandbox_id| self.0.contains(sandbox_id))
            .collect();
        self.0 = named.into_iter().collect();
        repeated
    }
}

#[derive(Clone)]
struct ReporterConfig {
    scheduler_endpoint: String,
    /// Re-read at runtime; once resolved, overrides the static scheduler endpoint.
    scheduler_endpoint_file: Option<PathBuf>,
    interval: Duration,
}

pub struct ObservabilityReporter {
    config: ReporterConfig,
    service: Arc<ObservabilityService>,
    channel_source: SchedulerEndpointSource,
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
        let channel_source = SchedulerEndpointSource::spawn(
            config.scheduler_endpoint.clone(),
            config
                .scheduler_endpoint_file
                .clone()
                .map(|path| (path, config.interval)),
            "heartbeat",
        )?;

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
        let channel_source = self.channel_source.clone();
        let event_channel_source = self.channel_source.clone();
        let ever_heartbeat_succeeded = Arc::clone(&self.ever_heartbeat_succeeded);
        let p2p_endpoint = self.p2p_endpoint.clone();
        let mut heartbeat_shutdown_rx = shutdown_rx.clone();
        let mut event_shutdown_rx = shutdown_rx;
        let mut sandbox_event_rx = event_service.subscribe_sandbox_events();

        let heartbeat_join = tokio::spawn(async move {
            let mut backoff = config.interval;
            let mut wait = Duration::from_millis(100);
            let mut pending_cpu_config_json = service.take_cpu_config_json();
            let mut disowned = DisownedCandidates::default();

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

                // Resolve every iteration so endpoint reloads affect the next heartbeat.
                let (scheduler_channel, current_endpoint) = channel_source.current();

                match Self::send_heartbeat(
                    &service,
                    &scheduler_channel,
                    &current_endpoint,
                    &mut pending_cpu_config_json,
                    p2p_endpoint.as_ref(),
                    &mut disowned,
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
                            node_id = %service.node_id(),
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
                        // Resolve every batch so events follow heartbeat endpoint reloads.
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
            node_id = %self.service.node_id(),
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
        disowned: &mut DisownedCandidates,
    ) -> Result<()> {
        let mut snapshot = service
            .node_snapshot()
            .await
            .context("failed to collect heartbeat snapshot")?;
        snapshot.machine_info.cpu_config_json = cpu_config_json.clone();
        let node_id = snapshot.node_id.clone();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let req = Self::build_heartbeat_request(snapshot, now_ms, p2p_endpoint);

        let mut primary_request = Request::new(req);
        primary_request.set_timeout(GRPC_CALL_TIMEOUT);
        let mut primary_client = SchedulerClient::new(scheduler_channel.clone());
        let primary_send = primary_client.heartbeat(primary_request);

        let response = primary_send
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
            info!(
                node_id = %node_id,
                "received cluster cpu config intersection from scheduler"
            );
        }

        // Ids this side cannot parse are dropped rather than guessed at: the
        // control plane names sandboxes, and anything else is not one.
        let named = response
            .disowned_sandbox_ids
            .iter()
            .filter_map(|raw| crate::types::SandboxId::parse_str(raw.trim()).ok())
            .collect();
        for sandbox_id in disowned.confirm(named) {
            service.discard_disowned_paused_sandbox(sandbox_id).await;
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
                    paused: entry.paused,
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
            // The receiver uses this incarnation to guard projection deletion.
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
        // Unregister from the scheduler currently receiving heartbeats.
        let (channel, _endpoint) = self.channel_source.current();
        self.unregister_node_on(channel).await
    }

    async fn unregister_node_on(&self, channel: Channel) -> Result<()> {
        let mut request = Request::new(scheduler::UnregisterNodeRequest {
            node_id: self.service.node_id().to_string(),
            service_instance_id: self.service.service_instance_id().to_string(),
        });
        request.set_timeout(GRPC_CALL_TIMEOUT);
        SchedulerClient::new(channel)
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

        let scheduler_endpoint_file =
            crate::scheduler_endpoint::resolve_endpoint_file(cluster_config);

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

    #[test]
    fn a_sandbox_named_once_is_not_discarded_until_the_next_heartbeat_names_it_again() {
        let mut candidates = DisownedCandidates::default();
        let sandbox_id = SandboxId::new();

        assert!(
            candidates.confirm(vec![sandbox_id]).is_empty(),
            "one naming can be the window between a node reporting a pause and the row for it \
             being written"
        );
        assert_eq!(candidates.confirm(vec![sandbox_id]), vec![sandbox_id]);
    }

    #[test]
    fn a_sandbox_the_control_plane_stops_naming_is_forgotten_rather_than_accumulated() {
        let mut candidates = DisownedCandidates::default();
        let sandbox_id = SandboxId::new();

        candidates.confirm(vec![sandbox_id]);
        assert!(candidates.confirm(Vec::new()).is_empty());
        assert!(
            candidates.confirm(vec![sandbox_id]).is_empty(),
            "two namings a heartbeat apart with a denial between them are not consecutive"
        );
    }

    #[test]
    fn confirming_names_only_the_sandboxes_named_twice() {
        let mut candidates = DisownedCandidates::default();
        let repeated = SandboxId::new();
        let fresh = SandboxId::new();

        candidates.confirm(vec![repeated]);
        assert_eq!(candidates.confirm(vec![repeated, fresh]), vec![repeated]);
    }

    pub fn make_cluster_config(endpoint: Option<&str>) -> ClusterConfig {
        make_cluster_config_with_file(endpoint, None)
    }

    pub fn make_cluster_config_with_file(
        endpoint: Option<&str>,
        scheduler_endpoint_file: Option<&str>,
    ) -> ClusterConfig {
        ClusterConfig {
            scheduler_endpoint: endpoint.map(|s| s.to_string()),
            scheduler_endpoint_file: scheduler_endpoint_file.unwrap_or_default().to_string(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            node_discovery_mode: Default::default(),
            kubernetes_discovery: Default::default(),
            static_discovery_nodes: Vec::new(),
            native_warmup_timeout_secs: 15,
            placement_shadow_k: crate::node_registry::placement::DEFAULT_PLACEMENT_SHADOW_K,
            node_registry_store: Default::default(),
        }
    }

    pub fn make_report_config(
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

    #[test]
    fn the_heartbeat_roster_carries_each_sandbox_budget() {
        let entry = SandboxRosterEntry {
            sandbox_id: SandboxId::new(),
            execution_id: ExecutionId::new(),
            projection_ttl_secs: 86_460,
            paused: false,
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

    #[test]
    fn a_roster_entry_without_a_budget_says_zero() {
        let entry = SandboxRosterEntry {
            sandbox_id: SandboxId::new(),
            execution_id: ExecutionId::new(),
            projection_ttl_secs: 0,
            paused: false,
        };

        let request =
            ObservabilityReporter::build_heartbeat_request(node_snapshot(vec![entry]), 0, None);

        assert_eq!(request.roster[0].projection_ttl_secs, 0);
    }

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
        let cluster = make_cluster_config_with_file(
            Some("http://scheduler:9090"),
            Some("  /etc/agentenv/heartbeat/scheduler-endpoint  "),
        );
        let cfg = make_report_config(Some(true), None);
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(
            result.scheduler_endpoint_file,
            Some(PathBuf::from("/etc/agentenv/heartbeat/scheduler-endpoint"))
        );
    }

    #[test]
    fn resolve_treats_a_blank_endpoint_file_as_unset() {
        let cluster = make_cluster_config_with_file(Some("http://scheduler:9090"), Some("   "));
        let cfg = make_report_config(Some(true), None);
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.scheduler_endpoint_file, None);
    }
}

#[cfg(test)]
mod against_a_scheduler {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration as StdDuration, Instant};

    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    use super::tests::{make_cluster_config_with_file, make_report_config};
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{Orchestrator, SandboxOrchestration};
    use crate::proto::scheduler::scheduler_server::{Scheduler, SchedulerServer};

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
        async fn report_sandbox_event(
            &self,
            _request: Request<scheduler::ReportSandboxEventRequest>,
        ) -> Result<Response<scheduler::ReportSandboxEventResponse>, Status> {
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
    }

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

    async fn test_service() -> Arc<ObservabilityService> {
        let orchestrator =
            Orchestrator::with_in_memory_store(crate::sandbox::mock::MockBackendFactory::new())
                .await;
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

    #[tokio::test]
    async fn a_hot_reloaded_endpoint_actually_moves_the_traffic() {
        let (scheduler_a, addr_a, _shutdown_a) = scheduler_on_a_socket().await;
        let (scheduler_b, addr_b, _shutdown_b) = scheduler_on_a_socket().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("scheduler-endpoint");
        std::fs::write(&file_path, format!("http://{addr_a}")).expect("seed the file with A");

        let service = test_service().await;
        let cluster = make_cluster_config_with_file(
            Some(&format!("http://{addr_a}")),
            Some(file_path.to_str().expect("temp paths are valid utf-8")),
        );
        let report_config = make_report_config(Some(true), Some(1));

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
