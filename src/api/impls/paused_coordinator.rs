//! Cluster bookkeeping for paused sandboxes.
//!
//! The orchestrator drives best-effort snapshot publication and registry updates
//! whenever a cluster-backed registry is configured.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::orchestrator::{
    DeadlineRenewalOutcome, MarkRunningOutcome, PauseOutcome, PausedRegistryState,
    PausedSandboxEntry, PausedSandboxPublisher, PausedSandboxRegistry, SandboxMetadata,
};
use crate::snapshot::{SnapshotId, SnapshotManager, SnapshotPublishMetadata};
use crate::types::{ExecutionId, SandboxId};

/// Running sandboxes whose registry holder write this process confirmed.
///
/// Reconciliation acts only on these process-local confirmations.
#[derive(Default)]
struct RunningRegistrations(Mutex<HashMap<SandboxId, String>>);

impl RunningRegistrations {
    /// Records a confirmed holder write without clearing earlier confirmation on refusal.
    fn observe(&self, sandbox_id: SandboxId, node_id: &str, confirmed: bool) {
        if !confirmed {
            return;
        }
        self.lock().insert(sandbox_id, node_id.to_string());
    }

    fn get(&self, sandbox_id: &SandboxId) -> Option<String> {
        self.lock().get(sandbox_id).cloned()
    }

    fn forget(&self, sandbox_id: &SandboxId) {
        self.lock().remove(sandbox_id);
    }

    fn retain(&self, live: &HashSet<SandboxId>) {
        self.lock()
            .retain(|sandbox_id, _| live.contains(sandbox_id));
    }

    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SandboxId, String>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Startup window for releasing rows held by the previous process.
///
/// It closes before any attempt to make a sandbox live and stays locked across
/// the release operation.
struct TakeoverWindow(tokio::sync::Mutex<bool>);

impl TakeoverWindow {
    fn new() -> Self {
        Self(tokio::sync::Mutex::new(true))
    }

    /// Enters the still-open takeover window.
    async fn enter(&self) -> Option<TakeoverAttempt<'_>> {
        let guard = self.0.lock().await;
        if !*guard {
            return None;
        }

        Some(TakeoverAttempt(guard))
    }

    /// Closes the takeover window permanently.
    async fn close(&self) {
        *self.0.lock().await = false;
    }
}

/// Guard that leaves the takeover window open unless explicitly settled.
struct TakeoverAttempt<'a>(tokio::sync::MutexGuard<'a, bool>);

impl TakeoverAttempt<'_> {
    fn settle(mut self) {
        *self.0 = false;
    }
}

/// Result of releasing a previous process's holdings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReleaseOutcome {
    /// The rows were handed back, or there were none. Nothing more to do.
    Released,
    /// The takeover window has closed — this process has taken a sandbox live,
    /// so releasing by node identity would now give away one of its own.
    /// Whatever was still stranded stays stranded until the next start.
    Fenced,
    /// The registry could not be reached. Worth retrying while the window is
    /// still open.
    Failed,
}

/// Whether and how a pause was represented in the cluster registry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterRecord {
    /// Row identity that reconciliation must compare with `origin_node_id`.
    Registered(String),
    /// No row, and why not.
    Unrecorded(Unrecorded),
}

/// Why a pause left no cluster row.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Unrecorded {
    /// Capture cannot outlive this process.
    NoDurableCapture,
    /// Durable capture belongs to another machine and this call has nothing to commit.
    HeldByAnotherMachine(String),
    /// No cluster registry exists to reference a published snapshot.
    NoClusterRegistry,
    /// The registry could not be reached.
    Unreachable,
}

impl Unrecorded {
    /// Stable metric label for this reason.
    fn reason(&self) -> &'static str {
        match self {
            Self::NoDurableCapture => "no_durable_capture",
            Self::HeldByAnotherMachine(_) => "held_by_another_machine",
            Self::NoClusterRegistry => "no_cluster_registry",
            Self::Unreachable => "unreachable",
        }
    }
}

impl ClusterRecord {
    /// Returns the identity used for a registered row, if any.
    fn registered_as(self) -> Option<String> {
        match self {
            Self::Registered(node_id) => Some(node_id),
            Self::Unrecorded(_) => None,
        }
    }
}

/// Owns the cluster's view of this node's paused sandboxes.
pub struct PausedSandboxCoordinator {
    registry: Arc<dyn PausedSandboxRegistry>,
    snapshot_manager: Arc<SnapshotManager>,
    node_id: String,
    running_registrations: RunningRegistrations,
    takeover_window: TakeoverWindow,
    /// Consecutive lease-renewal failures, reset by one successful renewal.
    consecutive_renew_failures: AtomicU64,
}

impl PausedSandboxCoordinator {
    pub fn new(
        registry: Arc<dyn PausedSandboxRegistry>,
        snapshot_manager: Arc<SnapshotManager>,
        node_id: String,
    ) -> Self {
        Self {
            registry,
            snapshot_manager,
            node_id,
            running_registrations: RunningRegistrations::default(),
            takeover_window: TakeoverWindow::new(),
            consecutive_renew_failures: AtomicU64::new(0),
        }
    }

    /// Records the current consecutive lease-renewal failure count.
    pub fn observe_lease_renewal(&self, renewed: bool) {
        let failures = if renewed {
            self.consecutive_renew_failures.store(0, Ordering::SeqCst);
            0
        } else {
            self.consecutive_renew_failures
                .fetch_add(1, Ordering::SeqCst)
                + 1
        };

        metrics::gauge!("agentenv_paused_registry_renew_consecutive_failures").set(failures as f64);
    }

    #[cfg(test)]
    pub fn consecutive_renew_failures(&self) -> u64 {
        self.consecutive_renew_failures.load(Ordering::SeqCst)
    }

    pub fn registry(&self) -> &Arc<dyn PausedSandboxRegistry> {
        &self.registry
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Closes startup takeover before trying to make a sandbox live.
    pub async fn note_taking_sandbox_live(&self) {
        self.takeover_window.close().await;
    }

    /// Enters the takeover window for one previous-process release attempt.
    async fn enter_takeover_window(&self) -> Option<TakeoverAttempt<'_>> {
        // Confirmations should already imply the takeover window was closed.
        if !self.running_registrations.is_empty() {
            return None;
        }

        self.takeover_window.enter().await
    }

    /// Best-effort publication and cluster registration for a completed local pause.
    ///
    /// Returns a reason whenever no row is created.
    async fn publish(&self, outcome: PauseOutcome) -> ClusterRecord {
        let sandbox_id = outcome.metadata.id;

        // Holding-node identity distinguishes a joined remote pause from no durable capture.
        let holding_node = outcome
            .metadata
            .paused_state
            .as_ref()
            .and_then(|state| state.holding_node_id())
            .map(str::to_string);

        let publishable = match outcome.publishable {
            Some(publishable) => publishable,
            // A joined or already-completed pause owns its capture and row elsewhere.
            None => {
                if let Some(holding_node) = holding_node {
                    return self
                        .unrecorded(sandbox_id, Unrecorded::HeldByAnotherMachine(holding_node));
                }

                // Backend-managed temporary capture cannot be recovered by another node.
                return self.unrecorded(sandbox_id, Unrecorded::NoDurableCapture);
            }
        };

        // Without a cluster registry, publishing would create an unreachable orphan.
        if !self.registry.is_cluster_backed() {
            return self.unrecorded(sandbox_id, Unrecorded::NoClusterRegistry);
        }

        // The row names the machine holding the bytes, not the deciding process.
        // Reuse this exact value for both the row and local registration stamp.
        let origin_node_id = holding_node.unwrap_or_else(|| self.node_id.clone());

        let entry = PausedSandboxEntry {
            sandbox_id,
            // Registry fills its configured cluster id.
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: origin_node_id.clone(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: Some(outcome.metadata.clone()),
            // Fence the pause against a newer incarnation.
            execution_id: Some(outcome.metadata.execution_id),
            paused_at: DateTime::<Utc>::from(outcome.metadata.created_at),
            updated_at: Utc::now(),
        };

        let began = match self.registry.begin_pause(&entry).await {
            Ok(began) => began,
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to register paused sandbox; it stays resumable on this node only"
                );

                return self.unrecorded(sandbox_id, Unrecorded::Unreachable);
            }
        };

        let published = self
            .snapshot_manager
            .publish_captured(publish_metadata(&outcome.metadata), publishable)
            .await;

        match published {
            Ok(record) => {
                let snapshot_id = record.id.clone();
                match self
                    .registry
                    .complete_pause(&sandbox_id, began.generation, &snapshot_id)
                    .await
                {
                    Ok(()) => {
                        info!(%sandbox_id, %snapshot_id, "paused sandbox is recoverable cluster-wide");

                        // Preserve the previous durable snapshot until the new commit completes.
                        if let Some(previous) = began.previous_snapshot_id {
                            if previous != snapshot_id {
                                self.delete_snapshot(
                                    &sandbox_id,
                                    &previous,
                                    "superseded by a newer pause",
                                )
                                .await;
                            }
                        }
                    }
                    Err(err) => {
                        // Re-read before collecting a snapshot whose completion response failed.
                        warn!(
                            error = %err,
                            %sandbox_id,
                            %snapshot_id,
                            "published paused snapshot but could not mark it durable"
                        );
                        let _ = self
                            .discard_unreferenced_snapshot(&sandbox_id, &snapshot_id)
                            .await;
                    }
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to publish paused snapshot; it stays resumable on this node only"
                );
                // Preserve the local-only row when publication fails.
                if let Err(err) = self
                    .registry
                    .mark_local_only(&sandbox_id, began.generation)
                    .await
                {
                    warn!(error = %err, %sandbox_id, "failed to mark the registry row local-only");
                }
            }
        }

        ClusterRecord::Registered(origin_node_id)
    }

    /// Records why a pause left no cluster row at the appropriate log level.
    fn unrecorded(&self, sandbox_id: SandboxId, reason: Unrecorded) -> ClusterRecord {
        metrics::counter!(
            "agentenv_paused_registry_pause_unrecorded_total",
            "reason" => reason.reason()
        )
        .increment(1);

        match &reason {
            Unrecorded::Unreachable => {
                // The call site already logged the concrete registry error.
            }
            Unrecorded::HeldByAnotherMachine(holding_node) => info!(
                %sandbox_id,
                holding_node = %holding_node,
                "this pause produced nothing to commit: the sandbox was already parked on that \
                 machine, and the row belongs to the pause that put it there"
            ),
            Unrecorded::NoClusterRegistry => info!(
                %sandbox_id,
                "no cluster registry is configured, so this pause published no snapshot: the \
                 sandbox is resumable from this node's own persisted record and nowhere else. \
                 Publishing one would have uploaded a capture nothing can reference and nothing \
                 deletes"
            ),
            Unrecorded::NoDurableCapture => debug!(
                %sandbox_id,
                "pause produced no capture that outlives this process; nothing to record cluster-wide"
            ),
        }

        ClusterRecord::Unrecorded(reason)
    }

    /// Idempotently releases rows held by the previous process during startup.
    pub async fn release_stale_holdings(&self) -> StaleReleaseOutcome {
        let Some(attempt) = self.enter_takeover_window().await else {
            return StaleReleaseOutcome::Fenced;
        };

        match self.registry.release_node_holdings(&self.node_id).await {
            Ok(released) => {
                attempt.settle();
                if !released.is_empty() {
                    metrics::counter!("agentenv_paused_registry_node_holdings_released_total")
                        .increment(released.released);
                    metrics::counter!("agentenv_paused_registry_node_holdings_discarded_total")
                        .increment(released.discarded);
                    info!(
                        node_id = %self.node_id,
                        released = released.released,
                        discarded = released.discarded,
                        "took over the sandboxes a previous process on this node was holding"
                    );
                }

                StaleReleaseOutcome::Released
            }
            Err(err) => {
                // Failed release remains safely retryable while the takeover window is open.
                metrics::counter!("agentenv_paused_registry_stale_release_failed_total")
                    .increment(1);
                warn!(
                    error = %err,
                    node_id = %self.node_id,
                    "could not release the previous process's holdings; \
                     sandboxes it was running stay unclaimable until a retry lands"
                );

                StaleReleaseOutcome::Failed
            }
        }
    }

    /// Records the sandbox as running under its claimant and actual machine holder.
    ///
    /// Only a confirmed write enrolls it for running reconciliation.
    pub async fn mark_sandbox_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    ) {
        self.note_taking_sandbox_live().await;

        let holder = holding_node_id.unwrap_or_else(|| self.node_id.clone());

        let confirmed = match self
            .registry
            .mark_running(
                &sandbox_id,
                &self.node_id,
                &holder,
                execution_id,
                expires_at,
            )
            .await
        {
            Ok(MarkRunningOutcome::HeldElsewhere) => {
                // Another claimant may now be running this already-started sandbox;
                // reconciliation decides which live copy must stand down.
                warn!(
                    %sandbox_id,
                    "the cluster holds this sandbox's resume claim elsewhere; two nodes may be running it"
                );

                false
            }
            Ok(outcome) => outcome.adopted(),
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to record the sandbox as running here; another node may keep a stale copy"
                );

                false
            }
        };

        self.running_registrations
            .observe(sandbox_id, &holder, confirmed);
    }

    /// Best-effort mirror of a clamped timeout extension into the cluster registry.
    ///
    /// The orchestrator's metadata remains authoritative for local eviction.
    pub async fn renew_sandbox_deadline(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) {
        match self
            .registry
            .renew_sandbox_deadline(&sandbox_id, execution_id, expires_at)
            .await
        {
            Ok(DeadlineRenewalOutcome::Renewed) => {}
            // Untracked or superseded rows are expected and not retryable here.
            Ok(other) => {
                debug!(%sandbox_id, outcome = ?other, "cluster registry deadline was not renewed");
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to mirror the extended timeout into the cluster registry; \
                     ReclaimExpiredHoldings may still judge this sandbox against a stale deadline \
                     if this node ever stops reporting"
                );
            }
        }
    }

    /// Returns the holder identity confirmed for this running sandbox, if any.
    pub fn running_registration(&self, sandbox_id: &SandboxId) -> Option<String> {
        self.running_registrations.get(sandbox_id)
    }

    /// Retains registrations for the current local roster.
    pub fn retain_running_registrations(&self, live: &HashSet<SandboxId>) {
        self.running_registrations.retain(live);
    }

    /// Removes a gone sandbox's row and snapshot.
    ///
    /// `holding_node_id` must name the machine actually stopped by this delete.
    pub async fn forget_sandbox(&self, sandbox_id: SandboxId, holding_node_id: Option<String>) {
        self.running_registrations.forget(&sandbox_id);

        let entry = match self.registry.get(&sandbox_id).await {
            Ok(Some(entry)) => entry,
            // Nothing recorded (the common case with a node-local registry).
            Ok(None) => return,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to read the paused registry row");

                return;
            }
        };

        // Fall back to this process only when no remote holder is known.
        let holder_node_id = holding_node_id.as_deref().unwrap_or(self.node_id.as_str());

        if let Some(holder) = live_elsewhere(&entry, &self.node_id, holder_node_id) {
            // Deleting a local copy must not clear another node's live row.
            warn!(
                %sandbox_id,
                holder,
                "not clearing the cluster record: the sandbox is live on another node"
            );

            return;
        }

        // Generation fencing makes the ownership check and removal one decision.
        match self.registry.remove(&sandbox_id, entry.generation).await {
            Ok(true) => {}
            Ok(false) => {
                // The row changed hands after the read; preserve it and its snapshot.
                warn!(
                    %sandbox_id,
                    "not clearing the cluster record: it changed hands while this delete was being decided"
                );

                return;
            }
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to clear the paused registry row");

                // Keep the snapshot: the row still points at it.
                return;
            }
        }

        if let Some(snapshot_id) = entry.snapshot_id {
            self.delete_snapshot(&sandbox_id, &snapshot_id, "sandbox deleted")
                .await;
        }
    }

    /// Deletes a newly published snapshot only after a registry reread confirms
    /// nothing references it; uncertainty preserves the snapshot.
    async fn discard_unreferenced_snapshot(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &SnapshotId,
    ) -> OrphanVerdict {
        let reread = self.registry.get(sandbox_id).await;
        let readable = reread.is_ok();
        let referenced = reread
            .as_ref()
            .ok()
            .map(|entry| snapshot_is_referenced(entry.as_ref(), snapshot_id));

        let verdict = orphan_verdict(readable, referenced.unwrap_or(false));
        match verdict {
            OrphanVerdict::Delete => {
                self.delete_snapshot(sandbox_id, snapshot_id, "never referenced by the registry")
                    .await
            }
            OrphanVerdict::Referenced => info!(
                %sandbox_id,
                %snapshot_id,
                "registry does reference the published snapshot after all; keeping it"
            ),
            OrphanVerdict::Unknown => error!(
                %sandbox_id,
                %snapshot_id,
                "cannot tell whether the published snapshot is referenced; \
                 leaving it in the repository for manual collection"
            ),
        }

        verdict
    }

    async fn delete_snapshot(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &SnapshotId,
        reason: &'static str,
    ) {
        match self.snapshot_manager.delete(snapshot_id.to_string()).await {
            Ok(()) => info!(%sandbox_id, %snapshot_id, reason, "deleted paused snapshot"),
            Err(err) => {
                warn!(error = ?err, %sandbox_id, %snapshot_id, reason, "failed to delete paused snapshot")
            }
        }
    }
}

#[async_trait]
impl PausedSandboxPublisher for PausedSandboxCoordinator {
    async fn publish_paused(&self, outcome: PauseOutcome) -> Option<String> {
        self.publish(outcome).await.registered_as()
    }

    /// Must match the publication gate so captures are not produced only to be dropped.
    fn wants_publishable_capture(&self) -> bool {
        self.registry.is_cluster_backed()
    }

    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    ) {
        self.mark_sandbox_running(sandbox_id, execution_id, expires_at, holding_node_id)
            .await;
    }

    async fn renew_deadline(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) {
        self.renew_sandbox_deadline(sandbox_id, execution_id, expires_at)
            .await;
    }

    async fn forget(&self, sandbox_id: SandboxId, holding_node_id: Option<String>) {
        self.forget_sandbox(sandbox_id, holding_node_id).await;
    }
}

/// Returns a different machine currently running or resuming the sandbox.
///
/// Running rows compare holder identity; resuming rows compare claimant identity.
fn live_elsewhere(
    entry: &PausedSandboxEntry,
    claimant_node_id: &str,
    holder_node_id: &str,
) -> Option<String> {
    match entry.state {
        PausedRegistryState::Running if entry.origin_node_id != holder_node_id => {
            Some(entry.origin_node_id.clone())
        }
        PausedRegistryState::Resuming => entry
            .claimed_by_node_id
            .as_ref()
            .filter(|claimer| *claimer != claimant_node_id)
            .cloned(),
        _ => None,
    }
}

/// What to do with a snapshot whose `complete_pause` did not report success.
#[derive(Debug, PartialEq, Eq)]
enum OrphanVerdict {
    /// Nothing references it: collect it.
    Delete,
    /// The row does reference it, so the write landed after all.
    Referenced,
    /// The registry could not answer.
    Unknown,
}

fn snapshot_is_referenced(entry: Option<&PausedSandboxEntry>, snapshot_id: &SnapshotId) -> bool {
    entry.is_some_and(|entry| entry.snapshot_id.as_ref() == Some(snapshot_id))
}

/// Chooses whether to collect a snapshot after an unacknowledged registry completion.
fn orphan_verdict(registry_readable: bool, referenced: bool) -> OrphanVerdict {
    match (registry_readable, referenced) {
        (false, _) => OrphanVerdict::Unknown,
        (true, true) => OrphanVerdict::Referenced,
        (true, false) => OrphanVerdict::Delete,
    }
}

/// Builds unnamed publication metadata for an internal pause snapshot.
fn publish_metadata(metadata: &SandboxMetadata) -> SnapshotPublishMetadata {
    crate::orchestrator::capture_publish_metadata(metadata, None)
}

#[cfg(test)]
mod tests {
    use super::test_support::{CountingRegistry, GetAnswer};
    use super::*;
    use crate::orchestrator::{
        BeganPause, HeldSandbox, PausedRegistryError, PausedRegistryRows, ReclaimedHoldings,
        RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::sandbox::CapturedSandboxSnapshot;
    use crate::snapshot::repository::{
        ImportedSnapshotArtifacts, SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit,
        SnapshotListFilter, SnapshotListPage, SnapshotRepository, StartedBuild,
    };
    use crate::snapshot::{
        PersistedDiskImagePublication, RepositoryResult, SnapshotRecord, TemplateBuildErrorReason,
    };
    use crate::types::FirecrackerSnapshotManifest;

    #[test]
    fn the_snapshot_a_sandbox_publishes_carries_no_execution() {
        let metadata = SandboxMetadata::default();
        let rendered = format!("{:?}", publish_metadata(&metadata));

        assert!(
            !rendered.contains(&metadata.execution_id.to_string()),
            "the published snapshot names the run that produced it: {rendered}"
        );
        assert!(
            !rendered.to_lowercase().contains("execution"),
            "the published snapshot has an incarnation-shaped field: {rendered}"
        );
    }

    #[test]
    fn unreferenced_snapshot_is_collected() {
        assert_eq!(orphan_verdict(true, false), OrphanVerdict::Delete);
    }

    #[test]
    fn snapshot_the_registry_points_at_is_kept() {
        assert_eq!(orphan_verdict(true, true), OrphanVerdict::Referenced);
    }

    #[test]
    fn unreadable_registry_never_deletes() {
        assert_eq!(orphan_verdict(false, false), OrphanVerdict::Unknown);
        assert_eq!(orphan_verdict(false, true), OrphanVerdict::Unknown);
    }

    #[test]
    fn an_unconfirmed_mark_leaves_the_sandbox_unregistered() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", false);

        assert_eq!(registrations.get(&sandbox_id), None);
    }

    #[test]
    fn a_confirmed_mark_records_the_identity_it_was_confirmed_under() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", true);

        assert_eq!(registrations.get(&sandbox_id), Some("node-a".to_string()));
    }

    #[test]
    fn a_refusal_does_not_clear_an_earlier_confirmation() {
        let registrations = RunningRegistrations::default();
        let sandbox_id = SandboxId::new();

        registrations.observe(sandbox_id, "node-a", true);
        registrations.observe(sandbox_id, "node-a", false);

        assert_eq!(registrations.get(&sandbox_id), Some("node-a".to_string()));
    }

    #[test]
    fn retain_drops_sandboxes_this_node_no_longer_has() {
        let registrations = RunningRegistrations::default();
        let kept = SandboxId::new();
        let dropped = SandboxId::new();
        registrations.observe(kept, "node-a", true);
        registrations.observe(dropped, "node-a", true);

        registrations.retain(&HashSet::from([kept]));

        assert_eq!(registrations.get(&kept), Some("node-a".to_string()));
        assert_eq!(registrations.get(&dropped), None);
    }

    fn entry(
        state: PausedRegistryState,
        origin: &str,
        claimed_by: Option<&str>,
    ) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id: SandboxId::new(),
            cluster_id: uuid::Uuid::nil(),
            state,
            generation: 1,
            origin_node_id: origin.to_string(),
            claimed_by_node_id: claimed_by.map(str::to_string),
            snapshot_id: Some(SnapshotId::generate()),
            metadata: Some(SandboxMetadata::default()),
            execution_id: Some(ExecutionId::new()),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn a_sandbox_running_on_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Running, "node-b", None),
                "node-b",
                "node-a",
            ),
            Some("node-b".to_string())
        );
    }

    #[test]
    fn a_sandbox_claimed_by_another_node_is_not_forgotten() {
        assert_eq!(
            live_elsewhere(
                &entry(PausedRegistryState::Resuming, "node-a", Some("node-b")),
                "node-a",
                "node-b",
            ),
            Some("node-b".to_string())
        );
    }

    #[test]
    fn our_own_sandbox_is_forgotten() {
        assert!(live_elsewhere(
            &entry(PausedRegistryState::Running, "node-a", None),
            "node-a",
            "node-a",
        )
        .is_none());
    }

    #[test]
    fn a_parked_sandbox_is_forgotten_from_any_node() {
        for state in [
            PausedRegistryState::Paused,
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
        ] {
            assert!(
                live_elsewhere(&entry(state, "node-b", None), "node-a", "node-a").is_none(),
                "{state:?} should be clearable from another node"
            );
        }
    }

    #[test]
    fn a_sandbox_this_delete_just_stopped_is_forgotten_even_though_the_claimant_is_never_a_real_node(
    ) {
        assert!(
            live_elsewhere(
                &entry(PausedRegistryState::Running, "aenv-master-01", None),
                // The api replica's own pod identity: structurally never
                // equal to any real node's name.
                "aenv-api-6c9f8d59b4-x7z2q",
                // What this delete's handle reported holding it on.
                "aenv-master-01",
            )
            .is_none(),
            "the row names the machine this delete just stopped it on; it must be forgotten"
        );
    }

    #[tokio::test]
    async fn forget_sandbox_clears_the_row_the_real_holder_just_stopped_it_on() {
        let sandbox_id = SandboxId::new();
        let mut seeded_row = entry(PausedRegistryState::Running, "real-node-1", None);
        seeded_row.sandbox_id = sandbox_id;
        // Keeps this test to the registry row alone; snapshot deletion has
        // its own coverage above.
        seeded_row.snapshot_id = None;
        let registry = Arc::new(RecordingRegistry {
            rows: Mutex::new(HashMap::from([(sandbox_id, seeded_row)])),
            ..RecordingRegistry::default()
        });
        let coordinator = recording_coordinator(Arc::clone(&registry));

        // THIS_REPLICA ("api-replica-1") is the coordinator's own claimant
        // identity, and structurally never equals a real node's name — the
        // exact split-topology shape that leaked every row on the dev
        // cluster. `holding_node_id` is what a real delete threads through
        // from the handle it just stopped, naming the same machine the row
        // already does.
        coordinator
            .forget_sandbox(sandbox_id, Some("real-node-1".to_string()))
            .await;

        assert!(
            registry.row(&sandbox_id).is_none(),
            "the registry row must actually be gone, not just have `forget_sandbox` return"
        );
    }

    #[tokio::test]
    async fn a_failed_complete_pause_rereads_the_row_before_touching_the_snapshot() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Referencing(
            snapshot_id.clone(),
        )));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &snapshot_id)
            .await;

        assert_eq!(registry.get_calls(), 1, "the row must be re-read");
        assert_eq!(
            verdict,
            OrphanVerdict::Referenced,
            "a complete_pause whose response was lost still applied the write"
        );
    }

    #[tokio::test]
    async fn an_unreadable_registry_leaves_the_snapshot_alone() {
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Unreachable));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &SnapshotId::generate())
            .await;

        assert_eq!(registry.get_calls(), 1);
        assert_eq!(verdict, OrphanVerdict::Unknown);
    }

    #[tokio::test]
    async fn a_snapshot_no_row_points_at_is_collected() {
        let registry = Arc::new(CountingRegistry::answering(GetAnswer::Missing));
        let coordinator = coordinator(Arc::clone(&registry));

        let verdict = coordinator
            .discard_unreferenced_snapshot(&SandboxId::new(), &SnapshotId::generate())
            .await;

        assert_eq!(registry.get_calls(), 1);
        assert_eq!(verdict, OrphanVerdict::Delete);
    }

    fn coordinator(registry: Arc<CountingRegistry>) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(
            registry,
            Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
            "node-a".to_string(),
        )
    }

    #[tokio::test]
    async fn renew_sandbox_deadline_forwards_what_it_was_given() {
        let registry = Arc::new(CountingRegistry::new(0, false));
        let coordinator = coordinator(Arc::clone(&registry));

        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let deadline =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);

        coordinator
            .renew_sandbox_deadline(sandbox_id, execution_id, Some(deadline))
            .await;

        assert_eq!(
            registry.renewed_deadlines(),
            vec![(sandbox_id, execution_id, Some(deadline))]
        );
    }

    #[tokio::test]
    async fn renew_sandbox_deadline_does_not_propagate_a_registry_failure() {
        let registry = Arc::new(CountingRegistry::failing_renew_deadline());
        let coordinator = coordinator(Arc::clone(&registry));

        // Must return `()` and must not panic, unwrap, or otherwise surface
        // the backend error to a caller with no way to act on it.
        coordinator
            .renew_sandbox_deadline(SandboxId::new(), ExecutionId::new(), None)
            .await;

        assert_eq!(
            registry.renewed_deadlines().len(),
            1,
            "the attempt must still have been made"
        );
    }

    #[tokio::test]
    async fn a_registry_failure_leaves_the_window_open_for_another_attempt() {
        let registry = Arc::new(CountingRegistry::new(1, false));
        let coordinator = coordinator(Arc::clone(&registry));

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Failed
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Released
        );
        assert_eq!(registry.release_calls(), 2);
    }

    #[tokio::test]
    async fn a_successful_release_closes_the_window() {
        let registry = Arc::new(CountingRegistry::new(0, false));
        let coordinator = coordinator(Arc::clone(&registry));

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Released
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 1);
    }

    #[tokio::test]
    async fn taking_a_sandbox_live_fences_the_release() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator.note_taking_sandbox_live().await;

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(
            registry.release_calls(),
            0,
            "a fenced release must not reach the registry at all"
        );
    }

    #[tokio::test]
    async fn an_unconfirmed_mark_running_still_fences_the_release() {
        let registry = Arc::new(CountingRegistry::new(usize::MAX, true));
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None, None)
            .await;

        assert_eq!(
            coordinator.running_registration(&SandboxId::new()),
            None,
            "an unacknowledged mark must not enrol anything"
        );
        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 0);
    }

    #[tokio::test]
    async fn pruning_the_registrations_does_not_reopen_the_window() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let coordinator = coordinator(Arc::clone(&registry));

        coordinator
            .mark_sandbox_running(SandboxId::new(), ExecutionId::new(), None, None)
            .await;
        coordinator.retain_running_registrations(&HashSet::new());

        assert_eq!(
            coordinator.release_stale_holdings().await,
            StaleReleaseOutcome::Fenced
        );
        assert_eq!(registry.release_calls(), 0);
    }

    /// Registry fixture that stores rows for readback.
    struct RecordingRegistry {
        rows: Mutex<HashMap<SandboxId, PausedSandboxEntry>>,
        begin_pause_fails: bool,
        /// Whether this fixture provides cluster-backed behavior.
        cluster_backed: bool,
    }

    impl Default for RecordingRegistry {
        fn default() -> Self {
            Self {
                rows: Mutex::default(),
                begin_pause_fails: false,
                cluster_backed: true,
            }
        }
    }

    impl RecordingRegistry {
        fn unreachable() -> Self {
            Self {
                begin_pause_fails: true,
                ..Self::default()
            }
        }

        fn node_local() -> Self {
            Self {
                cluster_backed: false,
                ..Self::default()
            }
        }

        fn row(&self, sandbox_id: &SandboxId) -> Option<PausedSandboxEntry> {
            self.rows.lock().unwrap().get(sandbox_id).cloned()
        }

        fn row_count(&self) -> usize {
            self.rows.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl PausedSandboxRegistry for RecordingRegistry {
        async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            if self.begin_pause_fails {
                return Err(PausedRegistryError::Backend {
                    operation: "begin_pause",
                    source: anyhow::anyhow!("registry is unreachable"),
                });
            }

            let mut row = entry.clone();
            row.generation = 1;
            row.state = PausedRegistryState::Publishing;
            self.rows.lock().unwrap().insert(entry.sandbox_id, row);

            Ok(BeganPause {
                generation: 1,
                previous_snapshot_id: None,
            })
        }

        async fn complete_pause(
            &self,
            sandbox_id: &SandboxId,
            generation: i64,
            snapshot_id: &SnapshotId,
        ) -> RegistryResult<()> {
            if let Some(row) = self.rows.lock().unwrap().get_mut(sandbox_id) {
                if row.generation == generation {
                    row.state = PausedRegistryState::Paused;
                    row.snapshot_id = Some(snapshot_id.clone());
                }
            }

            Ok(())
        }

        async fn mark_local_only(
            &self,
            sandbox_id: &SandboxId,
            generation: i64,
        ) -> RegistryResult<()> {
            if let Some(row) = self.rows.lock().unwrap().get_mut(sandbox_id) {
                if row.generation == generation {
                    row.state = PausedRegistryState::LocalOnly;
                }
            }

            Ok(())
        }

        async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            Ok(self.row(sandbox_id))
        }

        async fn get_many(&self, sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
            let rows = self.rows.lock().unwrap();

            // A stub that always answers for everything it was asked: it reads
            // an in-memory map, so there is no row it can fail to decode.
            Ok(PausedRegistryRows::fully_covering(
                sandbox_ids
                    .iter()
                    .filter_map(|id| rows.get(id).map(|row| (*id, row.clone())))
                    .collect(),
                sandbox_ids,
            ))
        }

        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            Ok(ResumeClaim::NotFound)
        }

        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            Ok(false)
        }

        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            Ok(0)
        }

        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            Ok(ReclaimedHoldings::default())
        }

        async fn mark_running(
            &self,
            sandbox_id: &SandboxId,
            node_id: &str,
            holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            let mut rows = self.rows.lock().unwrap();
            let Some(row) = rows.get_mut(sandbox_id) else {
                return Ok(MarkRunningOutcome::Untracked);
            };

            let eligible = match row.state {
                // ① cross-node: this write's claimant must be the one the
                // claim was taken under.
                PausedRegistryState::Resuming => row.claimed_by_node_id.as_deref() == Some(node_id),
                // ② local reopen: no claim was ever taken, so the claimant
                // must already be the row's origin.
                PausedRegistryState::Paused
                | PausedRegistryState::Publishing
                | PausedRegistryState::LocalOnly => {
                    row.origin_node_id == node_id && row.claimed_by_node_id.is_none()
                }
                // ③ a retried write: compared against the holder, since that
                // is what branch ① or ② already wrote into origin_node_id —
                // see markRunningFencedSQL's note on why this is the one
                // branch that reads holder_node_id instead of node_id.
                PausedRegistryState::Running => row.origin_node_id == holder_node_id,
            };

            if !eligible {
                return Ok(MarkRunningOutcome::HeldElsewhere);
            }

            row.origin_node_id = holder_node_id.to_string();
            row.claimed_by_node_id = None;
            row.state = PausedRegistryState::Running;
            row.generation += 1;

            Ok(MarkRunningOutcome::Adopted)
        }

        // Mirrors renewSandboxDeadlineSQL's own guard: `Running` and the same
        // incarnation this call names, nothing else. A row this fake's own
        // `mark_running` never stamped an incarnation onto (seeded directly by
        // a test, or adopted through a path that predates the incarnation
        // being tracked) is treated as matching whatever is asked, the same
        // way a freshly-seeded row has no incarnation to disagree with —
        // callers that care about the fencing itself seed `execution_id`
        // explicitly.
        async fn renew_sandbox_deadline(
            &self,
            sandbox_id: &SandboxId,
            execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<DeadlineRenewalOutcome> {
            let rows = self.rows.lock().unwrap();
            let Some(row) = rows.get(sandbox_id) else {
                return Ok(DeadlineRenewalOutcome::NotTracked);
            };
            let matches = row.state == PausedRegistryState::Running
                && row.execution_id.is_none_or(|on_row| on_row == execution_id);
            Ok(if matches {
                DeadlineRenewalOutcome::Renewed
            } else {
                DeadlineRenewalOutcome::Superseded
            })
        }

        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            Ok(ReleasedHoldings::default())
        }

        async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            match rows.get(sandbox_id) {
                Some(row) if row.generation == generation => {
                    rows.remove(sandbox_id);
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        fn is_cluster_backed(&self) -> bool {
            self.cluster_backed
        }

        async fn list_all(&self) -> RegistryResult<crate::orchestrator::PausedRegistryListing> {
            let sandboxes = self
                .rows
                .lock()
                .unwrap()
                .values()
                .map(|entry| crate::orchestrator::PausedRegistryListEntry {
                    sandbox_id: entry.sandbox_id,
                    cluster_id: entry.cluster_id,
                    state: entry.state,
                    generation: entry.generation,
                    origin_node_id: entry.origin_node_id.clone(),
                    claimed_by_node_id: entry.claimed_by_node_id.clone(),
                    snapshot_id: entry.snapshot_id.clone(),
                    paused_at: entry.paused_at,
                    updated_at: entry.updated_at,
                    // This fake's rows never carry lease/deadline state --
                    // nothing in this file's own tests reads either back.
                    lease_expires_at: None,
                    sandbox_expires_at: None,
                    execution_id: entry.execution_id,
                })
                .collect();
            Ok(crate::orchestrator::PausedRegistryListing {
                sandboxes,
                now: chrono::Utc::now(),
            })
        }
    }

    #[derive(Debug)]
    struct CapturedOn(Option<String>);

    impl crate::sandbox::PausedSandboxState for CapturedOn {
        fn encode(&self) -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }

        fn runtime_artifacts(&self) -> crate::sandbox::RuntimeArtifactSet {
            crate::sandbox::RuntimeArtifactSet::empty()
        }

        fn holding_node_id(&self) -> Option<&str> {
            self.0.as_deref()
        }
    }

    const THIS_REPLICA: &str = "api-replica-1";

    fn recording_coordinator(registry: Arc<RecordingRegistry>) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(
            registry,
            Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
            THIS_REPLICA.to_string(),
        )
    }

    fn pause_outcome(
        sandbox_id: SandboxId,
        committable: bool,
        holding_node: Option<&str>,
    ) -> PauseOutcome {
        let mut metadata = SandboxMetadata {
            id: sandbox_id,
            ..SandboxMetadata::default()
        };
        metadata.paused_state = Some(Arc::new(CapturedOn(holding_node.map(str::to_string))));

        PauseOutcome {
            metadata,
            // The concrete capture is never read on the paths under test: the
            // mock snapshot manager refuses to stage anything it did not
            // produce, which lands on the `mark_local_only` arm and leaves the
            // row where these tests can see it.
            publishable: committable.then(crate::sandbox::CapturedSandboxSnapshot::unpublishable),
        }
    }

    /// Journal of repository write-path calls.
    #[derive(Debug, Default)]
    struct RepositoryJournal(Mutex<Vec<String>>);

    impl RepositoryJournal {
        fn record(&self, entry: &str) {
            self.0.lock().expect("journal").push(entry.to_string());
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().expect("journal").clone()
        }
    }

    struct JournallingArtifacts(Arc<RepositoryJournal>);

    #[async_trait]
    impl SnapshotArtifactStore for JournallingArtifacts {
        async fn import_built_artifacts(
            &self,
            _metadata: &SnapshotPublishMetadata,
            _manifest: &FirecrackerSnapshotManifest,
            _publications: &mut Vec<PersistedDiskImagePublication>,
        ) -> RepositoryResult<ImportedSnapshotArtifacts> {
            self.0.record("artifacts.upload");

            Ok(ImportedSnapshotArtifacts::default())
        }

        async fn delete_artifacts(
            &self,
            _id: &SnapshotId,
            _publications: &[PersistedDiskImagePublication],
        ) {
            self.0.record("artifacts.delete");
        }
    }

    struct JournallingCatalog(Arc<RepositoryJournal>);

    #[async_trait]
    impl SnapshotCatalog for JournallingCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            self.0.record("catalog.create");

            Ok(record)
        }

        async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
            self.0.record("catalog.commit");
            let mut record =
                SnapshotRecord::template_waiting(commit.id, commit.alias.clone(), commit.resources);
            record.mark_committed(
                commit.alias,
                commit.resources,
                commit.committed,
                commit.source,
                0,
            );

            Ok(record)
        }

        async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            self.0.record("catalog.get");

            Ok(None)
        }

        async fn list_page(
            &self,
            _filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            Ok(SnapshotListPage::single(Vec::new()))
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            self.0.record("catalog.delete_record");

            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            unreachable!("a pause never starts a template build")
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            unreachable!("a pause never fails a template build")
        }
    }

    fn journalling_snapshot_manager(journal: Arc<RepositoryJournal>) -> Arc<SnapshotManager> {
        Arc::new(SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::on_node(
                Arc::new(JournallingCatalog(Arc::clone(&journal))),
                Arc::new(JournallingArtifacts(journal)),
                THIS_REPLICA.to_string(),
            )),
            Some(Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver)),
            None,
        ))
    }

    fn coordinator_over(
        registry: Arc<dyn PausedSandboxRegistry>,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> PausedSandboxCoordinator {
        PausedSandboxCoordinator::new(registry, snapshot_manager, THIS_REPLICA.to_string())
    }

    fn publishable_pause_outcome(sandbox_id: SandboxId) -> PauseOutcome {
        let mut metadata = SandboxMetadata {
            id: sandbox_id,
            ..SandboxMetadata::default()
        };
        metadata.paused_state = Some(Arc::new(CapturedOn(None)));

        PauseOutcome {
            metadata,
            publishable: Some(CapturedSandboxSnapshot::local(
                crate::snapshot::CallerOwnedArtifacts::new(FirecrackerSnapshotManifest::for_test(
                    32768,
                    &[],
                )),
            )),
        }
    }

    #[tokio::test]
    async fn a_pause_uploads_only_when_something_can_reference_what_it_uploads() {
        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        let cluster_registry = Arc::new(RecordingRegistry::default());
        let local_registry = Arc::new(RecordingRegistry::node_local());
        let cluster = coordinator_over(Arc::clone(&cluster_registry) as _, Arc::clone(&snapshots));
        let local = coordinator_over(Arc::clone(&local_registry) as _, Arc::clone(&snapshots));

        let referenced = SandboxId::new();
        let unreferenced = SandboxId::new();

        let with_cluster = cluster.publish(publishable_pause_outcome(referenced)).await;
        let without_cluster = local.publish(publishable_pause_outcome(unreferenced)).await;

        // The half that has somewhere to point: bytes, row, and a registry
        // entry naming the snapshot those bytes became.
        assert_eq!(
            with_cluster,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "a pause a cluster registry can reference must still be registered"
        );
        let row = cluster_registry
            .row(&referenced)
            .expect("a cluster-backed pause leaves a row");
        assert_eq!(row.state, PausedRegistryState::Paused);
        assert!(
            row.snapshot_id.is_some(),
            "the row must name the snapshot the upload produced, or the upload is orphaned \
             on this path too"
        );

        // The half that has nowhere to point: no bytes, no row, and a reason
        // that says which of the ways to leave no row this was.
        assert_eq!(
            without_cluster,
            ClusterRecord::Unrecorded(Unrecorded::NoClusterRegistry),
            "a pause with no cluster registry must say so, not report a row it never wrote"
        );
        assert!(
            local_registry.row(&unreferenced).is_none(),
            "a registry that tracks nothing cluster-wide holds no row"
        );

        assert_eq!(
            journal.entries(),
            vec!["artifacts.upload".to_string(), "catalog.commit".to_string()],
            "exactly one of the two pauses may reach the repository, and it is the one \
             whose upload something references"
        );
    }

    #[tokio::test]
    async fn the_registry_a_node_actually_wires_in_publishes_nothing_either() {
        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        let accepted = coordinator_over(
            Arc::new(RecordingRegistry::default()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        let disabled = coordinator_over(
            Arc::new(crate::orchestrator::DisabledPausedSandboxRegistry) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;

        assert_eq!(
            accepted,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "the pause that proves the repository is wired up must still be registered"
        );
        assert_eq!(
            disabled,
            ClusterRecord::Unrecorded(Unrecorded::NoClusterRegistry),
            "the real disabled backend must reach the same answer the flagged fake does"
        );
        assert_eq!(
            journal.entries(),
            vec!["artifacts.upload".to_string(), "catalog.commit".to_string()],
            "the disabled backend must add nothing to what the accepted pause uploaded"
        );
    }

    #[tokio::test]
    async fn each_way_of_leaving_no_row_is_counted_under_its_own_reason() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Held across the awaits, which works only because `#[tokio::test]`
        // runs a current-thread runtime.
        let guard = metrics::set_default_local_recorder(&recorder);

        let journal = Arc::new(RepositoryJournal::default());
        let snapshots = journalling_snapshot_manager(Arc::clone(&journal));

        coordinator_over(
            Arc::new(RecordingRegistry::default()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        coordinator_over(
            Arc::new(RecordingRegistry::node_local()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        coordinator_over(
            Arc::new(RecordingRegistry::unreachable()) as _,
            Arc::clone(&snapshots),
        )
        .publish(publishable_pause_outcome(SandboxId::new()))
        .await;
        drop(guard);

        let mut counted: Vec<(String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let key = composite.key();
                if key.name() != "agentenv_paused_registry_pause_unrecorded_total" {
                    return None;
                }
                let reason = key
                    .labels()
                    .find(|label| label.key() == "reason")?
                    .value()
                    .to_string();
                match value {
                    DebugValue::Counter(count) => Some((reason, count)),
                    other => panic!("the unrecorded reason must be a counter, not {other:?}"),
                }
            })
            .collect();
        counted.sort();

        assert_eq!(
            counted,
            vec![
                ("no_cluster_registry".to_string(), 1),
                ("unreachable".to_string(), 1),
            ],
            "a pause that was registered must count nothing, and the two that were not must \
             be told apart by their label"
        );
    }

    #[tokio::test]
    async fn only_the_pause_this_half_can_commit_leaves_a_row() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let committable = SandboxId::new();
        let elsewhere = SandboxId::new();

        let committed = coordinator
            .publish(pause_outcome(committable, true, None))
            .await;
        let parked = coordinator
            .publish(pause_outcome(elsewhere, false, Some("node-7")))
            .await;

        assert_eq!(
            committed,
            ClusterRecord::Registered(THIS_REPLICA.to_string()),
            "a capture this half can commit must be registered under this identity"
        );
        assert!(
            registry.row(&committable).is_some(),
            "a capture this half can commit must leave the cluster a row"
        );

        assert_eq!(
            parked,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine("node-7".to_string())),
            "a capture on another machine must say so rather than report nothing"
        );
        assert!(
            registry.row(&elsewhere).is_none(),
            "a capture on another machine leaves no row on this half yet"
        );
        assert_eq!(
            registry.row_count(),
            1,
            "exactly one of the two pauses reached the registry"
        );
    }

    #[tokio::test]
    async fn out_of_reach_and_nothing_to_record_are_different_answers() {
        let unreachable_registry = Arc::new(RecordingRegistry::unreachable());
        let unreachable = recording_coordinator(Arc::clone(&unreachable_registry));
        let reachable_registry = Arc::new(RecordingRegistry::default());
        let reachable = recording_coordinator(Arc::clone(&reachable_registry));

        let out_of_reach = unreachable
            .publish(pause_outcome(SandboxId::new(), true, None))
            .await;
        let nothing_to_record = reachable
            .publish(pause_outcome(SandboxId::new(), false, None))
            .await;

        assert_eq!(
            out_of_reach,
            ClusterRecord::Unrecorded(Unrecorded::Unreachable)
        );
        assert_eq!(
            nothing_to_record,
            ClusterRecord::Unrecorded(Unrecorded::NoDurableCapture)
        );
        assert_ne!(
            out_of_reach, nothing_to_record,
            "a registry that could not be reached must not read as a pause with nothing to record"
        );
        assert_eq!(
            unreachable_registry.row_count(),
            0,
            "a registry that refused the write holds no row"
        );
        assert_eq!(
            reachable_registry.row_count(),
            0,
            "a pause with nothing durable behind it writes no row to a registry that would take one"
        );
    }

    #[tokio::test]
    async fn a_parked_pause_names_the_machine_the_bytes_are_on() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let record = coordinator
            .publish(pause_outcome(SandboxId::new(), false, Some("node-203")))
            .await;

        assert_eq!(
            record,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine("node-203".to_string()))
        );
        assert_ne!(
            record,
            ClusterRecord::Unrecorded(Unrecorded::HeldByAnotherMachine(THIS_REPLICA.to_string())),
            "the machine holding the bytes is not the replica that drove the pause"
        );
    }

    #[tokio::test]
    async fn a_row_names_the_machine_holding_the_bytes_not_the_replica_that_wrote_it() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let staged_on_a_node = SandboxId::new();
        let captured_here = SandboxId::new();

        // `aenv-api`: the node staged the bytes and handed back a capture to
        // commit, so both the holding machine and something to commit are
        // present. That pair is what the early return above no longer catches.
        let remote = coordinator
            .publish(pause_outcome(staged_on_a_node, true, Some("node-203")))
            .await;
        // the pre-split single process: captured in this process, so the paused state names no
        // other machine.
        let local = coordinator
            .publish(pause_outcome(captured_here, true, None))
            .await;

        let remote_row = registry
            .row(&staged_on_a_node)
            .expect("a capture this half can commit leaves a row");
        let local_row = registry
            .row(&captured_here)
            .expect("a capture this half can commit leaves a row");

        assert_eq!(
            remote_row.origin_node_id, "node-203",
            "the row must name the machine whose disk the bytes are on"
        );
        assert_ne!(
            remote_row.origin_node_id, THIS_REPLICA,
            "a row naming this replica pins the sandbox to a process that never heartbeats, \
             and the scheduler refuses every resume of it"
        );
        assert_eq!(
            local_row.origin_node_id, THIS_REPLICA,
            "a capture taken in this process is held by this process, and the rollback target \
             must keep writing exactly that"
        );
        assert_ne!(
            remote_row.origin_node_id, local_row.origin_node_id,
            "the two halves must not be able to agree: a build that stated one identity for \
             both would pass every assertion above that is about only one of them"
        );

        assert_eq!(remote, ClusterRecord::Registered("node-203".to_string()));
        assert_eq!(local, ClusterRecord::Registered(THIS_REPLICA.to_string()));
    }

    #[tokio::test]
    async fn a_resumed_row_names_the_machine_that_ran_it_not_the_replica_that_wrote_it() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let resumed_elsewhere = SandboxId::new();
        let resumed_here = SandboxId::new();

        // Seed a row for each sandbox the way a pause would — `mark_running`
        // never creates one, matching the real registry's contract.
        coordinator
            .publish(pause_outcome(resumed_elsewhere, true, None))
            .await;
        coordinator
            .publish(pause_outcome(resumed_here, true, None))
            .await;

        // Captured before mark_running, so the conjunction below can tell "the
        // generation actually moved" apart from "the row happens to already
        // look like this".
        let generation_before = registry
            .row(&resumed_elsewhere)
            .expect("publish left a row behind")
            .generation;

        // `aenv-api`: the resume claim landed on a machine, and the backend
        // that drove it knows which one.
        coordinator
            .mark_sandbox_running(
                resumed_elsewhere,
                ExecutionId::new(),
                None,
                Some("node-203".to_string()),
            )
            .await;
        // the pre-split single process: the backend ran in this process, so it has nothing to
        // report but `None` — which this process's own identity answers for.
        coordinator
            .mark_sandbox_running(resumed_here, ExecutionId::new(), None, None)
            .await;

        let remote_row = registry
            .row(&resumed_elsewhere)
            .expect("mark_running updates the row a pause left behind");
        let local_row = registry
            .row(&resumed_here)
            .expect("mark_running updates the row a pause left behind");

        // State, generation, holder, and cleared claimant together prove the write landed.
        assert_eq!(
            remote_row.state,
            PausedRegistryState::Running,
            "a refused write leaves the row in whatever state publish left it, never running"
        );
        assert_eq!(
            remote_row.generation,
            generation_before + 1,
            "the write must have actually landed, not merely left the row looking unchanged"
        );
        assert_eq!(
            remote_row.origin_node_id, "node-203",
            "the row must name the machine the VM actually started on"
        );
        assert!(
            remote_row.claimed_by_node_id.is_none(),
            "an adopted write clears the claim; a lingering claimed_by_node_id is the row a \
             refused mark_running leaves behind, still 'resuming'-shaped"
        );
        assert_ne!(
            remote_row.origin_node_id, THIS_REPLICA,
            "a row naming this replica pins the sandbox to a process that never heartbeats, \
             and the scheduler refuses every resume of it"
        );
        assert_eq!(
            local_row.origin_node_id, THIS_REPLICA,
            "a sandbox this process ran itself is correctly named by this process's own \
             identity, and the rollback target must keep writing exactly that"
        );
        assert_ne!(
            remote_row.origin_node_id, local_row.origin_node_id,
            "the two halves must not be able to agree: a build that stated one identity for \
             both would pass every assertion above that is about only one of them"
        );

        // Reconciliation must register the same holder identity written into the row.
        assert_eq!(
            coordinator.running_registration(&resumed_elsewhere),
            Some("node-203".to_string()),
            "the confirmed registration must be the machine the row names, not this replica"
        );
        assert_eq!(
            coordinator.running_registration(&resumed_here),
            Some(THIS_REPLICA.to_string())
        );
    }

    #[tokio::test]
    async fn the_stamp_handed_back_is_the_identity_the_row_carries() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let staged_on_a_node = SandboxId::new();
        let captured_here = SandboxId::new();

        let remote_stamp = coordinator
            .publish_paused(pause_outcome(staged_on_a_node, true, Some("node-204")))
            .await;
        let local_stamp = coordinator
            .publish_paused(pause_outcome(captured_here, true, None))
            .await;

        let remote_row = registry.row(&staged_on_a_node).expect("a row");
        let local_row = registry.row(&captured_here).expect("a row");

        assert_eq!(
            remote_stamp.as_deref(),
            Some(remote_row.origin_node_id.as_str()),
            "the identity stamped on the local record must be the one the row carries"
        );
        assert_eq!(
            local_stamp.as_deref(),
            Some(local_row.origin_node_id.as_str()),
            "and the same on the half where the two happen to be this process"
        );
        assert_ne!(
            remote_stamp, local_stamp,
            "the two stamps must differ, or neither assertion above is about the value under test"
        );
    }

    #[tokio::test]
    async fn only_a_written_row_is_handed_back_for_the_local_record() {
        let registry = Arc::new(RecordingRegistry::default());
        let coordinator = recording_coordinator(Arc::clone(&registry));

        let registered = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), true, None))
            .await;
        let parked = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), false, Some("node-7")))
            .await;
        let nothing = coordinator
            .publish_paused(pause_outcome(SandboxId::new(), false, None))
            .await;

        assert_eq!(registered, Some(THIS_REPLICA.to_string()));
        assert_eq!(parked, None);
        assert_eq!(nothing, None);

        let unreachable_registry = Arc::new(RecordingRegistry::unreachable());
        let unreachable = recording_coordinator(Arc::clone(&unreachable_registry));
        assert_eq!(
            unreachable
                .publish_paused(pause_outcome(SandboxId::new(), true, None))
                .await,
            None,
            "a registry that could not be reached must not stamp the local record either"
        );

        let local_registry = Arc::new(RecordingRegistry::node_local());
        let node_local = recording_coordinator(Arc::clone(&local_registry));
        assert_eq!(
            node_local
                .publish_paused(pause_outcome(SandboxId::new(), true, None))
                .await,
            None,
            "a pause with no cluster registry must not stamp the local record as announced"
        );
    }
}

#[cfg(test)]
pub mod test_support {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::orchestrator::{
        BeganPause, ConflictReason, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome,
        PausedRegistryError, PausedRegistryRows, PausedSandboxEntry, PausedSandboxRegistry,
        ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::snapshot::SnapshotId;
    use crate::types::{ExecutionId, SandboxId};

    /// Cluster-backed registry fixture with programmable failures and call counts.
    pub struct CountingRegistry {
        release_calls: AtomicUsize,
        releases_to_fail: usize,
        mark_running_fails: bool,
        get_calls: AtomicUsize,
        get_answer: GetAnswer,
        claim_fails: bool,
        renew_calls: AtomicUsize,
        renewals_to_fail: usize,
        remove_calls: AtomicUsize,
        /// Claimant identities observed in order.
        claimed_as: std::sync::Mutex<Vec<String>>,
        /// Optional conflict owner returned by `claim_for_resume`.
        conflict_origin: Option<String>,
        renew_deadline_fails: bool,
        /// Deadline-renewal calls observed by the fixture.
        renewed_deadlines:
            std::sync::Mutex<Vec<(SandboxId, ExecutionId, Option<std::time::SystemTime>)>>,
    }

    /// Programmed response for registry reads.
    pub enum GetAnswer {
        Missing,
        Referencing(SnapshotId),
        Unreachable,
    }

    impl CountingRegistry {
        pub fn new(releases_to_fail: usize, mark_running_fails: bool) -> Self {
            Self {
                release_calls: AtomicUsize::new(0),
                releases_to_fail,
                mark_running_fails,
                get_calls: AtomicUsize::new(0),
                get_answer: GetAnswer::Missing,
                claim_fails: false,
                renew_calls: AtomicUsize::new(0),
                renewals_to_fail: 0,
                remove_calls: AtomicUsize::new(0),
                claimed_as: std::sync::Mutex::new(Vec::new()),
                conflict_origin: None,
                renew_deadline_fails: false,
                renewed_deadlines: std::sync::Mutex::new(Vec::new()),
            }
        }

        pub fn answering_conflict(origin_node_id: &str) -> Self {
            Self {
                conflict_origin: Some(origin_node_id.to_string()),
                ..Self::new(0, false)
            }
        }

        pub fn unreachable() -> Self {
            Self::unreachable_for(usize::MAX)
        }

        pub fn unreachable_for(renewals_to_fail: usize) -> Self {
            Self {
                claim_fails: true,
                renewals_to_fail,
                get_answer: GetAnswer::Unreachable,
                ..Self::new(usize::MAX, true)
            }
        }

        pub fn always_failing() -> Self {
            Self::new(usize::MAX, false)
        }

        pub fn answering(get_answer: GetAnswer) -> Self {
            Self {
                get_answer,
                ..Self::new(0, false)
            }
        }

        pub fn failing_renew_deadline() -> Self {
            Self {
                renew_deadline_fails: true,
                ..Self::new(0, false)
            }
        }

        pub fn release_calls(&self) -> usize {
            self.release_calls.load(Ordering::SeqCst)
        }

        pub fn remove_calls(&self) -> usize {
            self.remove_calls.load(Ordering::SeqCst)
        }

        pub fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }

        pub fn claimed_as(&self) -> Vec<String> {
            self.claimed_as.lock().unwrap().clone()
        }

        pub fn renewed_deadlines(
            &self,
        ) -> Vec<(SandboxId, ExecutionId, Option<std::time::SystemTime>)> {
            self.renewed_deadlines.lock().unwrap().clone()
        }
    }

    fn unreachable_backend(operation: &'static str) -> PausedRegistryError {
        PausedRegistryError::Backend {
            operation,
            source: anyhow::anyhow!("registry is unreachable"),
        }
    }

    #[async_trait]
    impl PausedSandboxRegistry for CountingRegistry {
        async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            Ok(BeganPause {
                generation: 0,
                previous_snapshot_id: None,
            })
        }

        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &SnapshotId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);

            match &self.get_answer {
                GetAnswer::Missing => Ok(None),
                GetAnswer::Unreachable => Err(unreachable_backend("get")),
                GetAnswer::Referencing(snapshot_id) => Ok(Some(PausedSandboxEntry {
                    sandbox_id: *sandbox_id,
                    cluster_id: uuid::Uuid::nil(),
                    state: crate::orchestrator::PausedRegistryState::Paused,
                    generation: 1,
                    origin_node_id: "node-a".to_string(),
                    claimed_by_node_id: None,
                    snapshot_id: Some(snapshot_id.clone()),
                    metadata: Some(crate::orchestrator::SandboxMetadata::default()),
                    execution_id: Some(ExecutionId::new()),
                    paused_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                })),
            }
        }

        async fn get_many(&self, sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
            Ok(PausedRegistryRows::fully_covering(
                HashMap::new(),
                sandbox_ids,
            ))
        }

        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            self.claimed_as.lock().unwrap().push(node_id.to_string());

            if self.claim_fails {
                return Err(unreachable_backend("claim_for_resume"));
            }

            if let Some(origin_node_id) = &self.conflict_origin {
                return Ok(ResumeClaim::Conflict {
                    origin_node_id: origin_node_id.clone(),
                    reason: ConflictReason::Unspecified,
                });
            }

            Ok(ResumeClaim::NotFound)
        }

        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            Ok(true)
        }

        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            let seen = self.renew_calls.fetch_add(1, Ordering::SeqCst);
            if seen < self.renewals_to_fail {
                return Err(unreachable_backend("renew_lease"));
            }

            Ok(0)
        }

        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            Ok(ReclaimedHoldings::default())
        }

        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<std::time::SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            if self.mark_running_fails {
                return Err(unreachable_backend("mark_running"));
            }

            Ok(MarkRunningOutcome::Adopted)
        }

        async fn renew_sandbox_deadline(
            &self,
            sandbox_id: &SandboxId,
            execution_id: ExecutionId,
            expires_at: Option<std::time::SystemTime>,
        ) -> RegistryResult<DeadlineRenewalOutcome> {
            self.renewed_deadlines
                .lock()
                .unwrap()
                .push((*sandbox_id, execution_id, expires_at));

            if self.renew_deadline_fails {
                return Err(unreachable_backend("renew_sandbox_deadline"));
            }

            Ok(DeadlineRenewalOutcome::Renewed)
        }

        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            let seen = self.release_calls.fetch_add(1, Ordering::SeqCst);
            if seen < self.releases_to_fail {
                return Err(unreachable_backend("release_node_holdings"));
            }

            Ok(ReleasedHoldings::default())
        }

        async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
            self.remove_calls.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }

        async fn list_all(&self) -> RegistryResult<crate::orchestrator::PausedRegistryListing> {
            // Not one of the two calls this double is built around -- answers
            // the way the disabled registry does, per this struct's own doc.
            Ok(crate::orchestrator::PausedRegistryListing {
                sandboxes: Vec::new(),
                now: chrono::Utc::now(),
            })
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }
    }
}
