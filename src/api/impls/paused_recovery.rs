//! Cross-node resume and local-record reconciliation.
//!
//! Cluster-backed resumes rebuild from shared snapshots, while periodic
//! reconciliation removes local claims superseded by cluster truth.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use super::{ApiImpl, StaleReleaseOutcome};
use crate::orchestrator::{
    ClaimedExecution, ClusterRegistration, CreateSandboxRequest, HeldSandbox, NewTimeout,
    PausedRegistryState, PausedSandboxEntry, ResumeClaim, SandboxExpiry, SandboxLaunchSource,
    SandboxListFilter, SandboxMetadata, SandboxState,
};
use crate::snapshot::{CatalogReadScope, SnapshotAbsence};
use crate::types::{ExecutionId, SandboxId};

/// Outcome when this node has neither a local copy nor a resume claim.
///
/// Only confirmed cluster absence may become `Unknown` and downstream 404.
pub enum MissingLocalResume {
    /// The cluster confirms no such sandbox.
    Unknown,
    /// A competing resume completed locally.
    Resumed(Box<SandboxMetadata>),
    /// Another transition or holder owns the sandbox.
    Busy { holder: String },
    /// The registry could not determine the answer.
    Undecided(String),
    /// A rebuild from the row's published snapshot was attempted and failed.
    Failed(String),
}

/// Pure classification of registry and local-store observations.
#[derive(Debug, PartialEq, Eq)]
enum MissingLocalVerdict {
    Unknown,
    Ready,
    /// The row is parked with a published snapshot, so any claimant can rebuild it.
    Rebuildable,
    Wait {
        holder: String,
    },
    Busy {
        holder: String,
    },
}

/// Classifies a missing local resume without converting uncertainty into absence.
///
/// Parked rows are classified by what their snapshot allows, never by whether
/// `origin_node_id` happens to match the reader: that comparison is an identity
/// mismatch in a split deployment, where the claimant is a process and the
/// origin is a machine.
fn missing_local_verdict(
    entry: Option<&PausedSandboxEntry>,
    local: Option<&SandboxMetadata>,
    node_id: &str,
) -> MissingLocalVerdict {
    let Some(entry) = entry else {
        return MissingLocalVerdict::Unknown;
    };

    if local.is_some_and(|m| m.state == SandboxState::Running) {
        return MissingLocalVerdict::Ready;
    }

    if entry.state == PausedRegistryState::Paused && entry.snapshot_id.is_some() {
        return MissingLocalVerdict::Rebuildable;
    }

    let holder = entry
        .claimed_by_node_id
        .clone()
        .unwrap_or_else(|| entry.origin_node_id.clone());

    match entry.state {
        PausedRegistryState::Resuming if holder == node_id => MissingLocalVerdict::Wait { holder },
        _ => MissingLocalVerdict::Busy { holder },
    }
}

/// Outcome of rebuilding a sandbox from a claim this node already holds.
#[derive(Debug)]
pub enum CrossNodeResume {
    /// The sandbox is running here again, under its original ID.
    Restored(Box<SandboxMetadata>),
    /// The registry named a snapshot the repository no longer has, so there is
    /// nothing left to rebuild from.
    NotFound,
    /// Rebuilding failed.
    Failed(String),
}

/// Cluster arbitration shared by local and snapshot-backed resume paths.
pub(in crate::api) enum ResumeArbitration {
    /// Local truth may proceed under the returned incarnation.
    Proceed(ClaimedExecution),
    /// This node holds the registry claim and must release it on failure.
    Held(Box<PausedSandboxEntry>, ClaimedExecution),
    /// The newest snapshot is still being published by another node, which is
    /// therefore the only node that can serve this resume.
    NotReady { origin_node_id: String },
    /// Another node holds the sandbox, so resuming here would make a second
    /// live copy of it.
    Blocked { origin_node_id: String },
    /// The registry could not be reached about a sandbox it is known to have a
    /// say over, so nobody can tell whether resuming here would make a second
    /// live copy. Retryable, and never an answer about whether the sandbox
    /// exists.
    Unavailable { reason: String },
}

/// Why a local paused record is superseded.
enum Superseded {
    /// The cluster moved past this sandbox.
    Gone,
    /// Another node holds it.
    HeldBy(String),
    /// Another node is resuming it.
    ClaimedBy(String),
}

impl Superseded {
    fn reason(&self) -> String {
        match self {
            Self::Gone => "the cluster no longer knows this sandbox".to_string(),
            Self::HeldBy(node) => format!("node '{node}' holds it now"),
            Self::ClaimedBy(node) => format!("node '{node}' is resuming it"),
        }
    }
}

impl ApiImpl {
    /// Acquires the cluster's resume decision and claim for this sandbox.
    ///
    /// Registry outages fail closed for records previously announced to the cluster.
    pub(in crate::api) async fn arbitrate_resume(
        &self,
        sandbox_id: SandboxId,
    ) -> ResumeArbitration {
        // Mint before claiming so the resuming row is fenced by the new incarnation.
        let proposed = ExecutionId::new();

        if !self.paused.registry().is_cluster_backed() {
            // With no cluster registry, local truth is authoritative.
            return ResumeArbitration::Proceed(ClaimedExecution::from_claim(proposed));
        }

        // A successful or lost claim response can already have written this node's name.
        self.paused.note_taking_sandbox_live().await;

        // Claims use this deciding process's identity so replicas cannot confuse
        // another replica's claim with their own.
        let claimant = self.paused.node_id().to_string();

        let claim = match self
            .paused
            .registry()
            .claim_for_resume(&sandbox_id, &claimant, proposed)
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                // Fall back only according to what the local record proves.
                let registration = self
                    .orchestrator
                    .paused_record_cluster_registration(sandbox_id)
                    .await
                    .ok();

                return unreachable_arbitration(
                    registration,
                    sandbox_id,
                    &err.to_string(),
                    proposed,
                );
            }
        };

        // Classify the response under the same identity used to claim.
        arbitration(claim, &claimant, proposed)
    }

    const MISSING_LOCAL_WAIT: Duration = Duration::from_secs(5);
    const MISSING_LOCAL_POLL: Duration = Duration::from_millis(200);

    /// Retry cadence while startup release remains safely fenced.
    const STALE_RELEASE_RETRY_INTERVAL: Duration = Duration::from_secs(5);

    pub async fn resolve_missing_local_resume(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> MissingLocalResume {
        if !self.paused.registry().is_cluster_backed() {
            return MissingLocalResume::Unknown;
        }

        let node_id = self.paused.node_id().to_string();
        let deadline = Instant::now() + Self::MISSING_LOCAL_WAIT;
        let mut holder = node_id.clone();

        loop {
            let entry = match self.paused.registry().get(&sandbox_id).await {
                Ok(entry) => entry,
                // Registry errors remain undecided, never absent.
                Err(err) => return MissingLocalResume::Undecided(err.to_string()),
            };
            let local = match self.orchestrator.get_sandbox(&sandbox_id).await {
                Ok(local) => local,
                Err(err) => return MissingLocalResume::Undecided(err.to_string()),
            };

            match missing_local_verdict(entry.as_ref(), local.as_ref(), &node_id) {
                MissingLocalVerdict::Unknown => return MissingLocalResume::Unknown,
                MissingLocalVerdict::Ready => {
                    return match local {
                        Some(metadata) => MissingLocalResume::Resumed(Box::new(metadata)),
                        // Defensive fallback for an inconsistent observation pair.
                        None => MissingLocalResume::Busy { holder },
                    };
                }
                MissingLocalVerdict::Rebuildable => {
                    return self.claim_and_restore(sandbox_id, timeout).await
                }
                MissingLocalVerdict::Busy { holder } => return MissingLocalResume::Busy { holder },
                MissingLocalVerdict::Wait { holder: who } => {
                    holder = who;
                    if Instant::now() >= deadline {
                        // Timeout remains retryable rather than becoming absence.
                        warn!(
                            %sandbox_id,
                            holder,
                            "another resume on this node has not landed within the wait window; \
                             reporting it as busy rather than missing"
                        );

                        return MissingLocalResume::Busy { holder };
                    }
                    tokio::time::sleep(Self::MISSING_LOCAL_POLL).await;
                }
            }
        }
    }

    /// Releases a resume claim after the corresponding resume failed.
    pub(in crate::api) async fn abandon_claim(&self, sandbox_id: SandboxId, generation: i64) {
        self.release_claim(&sandbox_id, generation).await;
    }

    /// Takes a claim on a parked row this process holds no copy of and rebuilds it.
    ///
    /// The claim CAS is the mutual exclusion: a caller that loses it reports the
    /// winner rather than starting a second copy.
    async fn claim_and_restore(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> MissingLocalResume {
        let entry = match self.arbitrate_resume(sandbox_id).await {
            ResumeArbitration::Held(entry, _) => entry,
            // No row left to rebuild from.
            ResumeArbitration::Proceed(_) => return MissingLocalResume::Unknown,
            ResumeArbitration::Blocked { origin_node_id }
            | ResumeArbitration::NotReady { origin_node_id } => {
                return MissingLocalResume::Busy {
                    holder: origin_node_id,
                }
            }
            ResumeArbitration::Unavailable { reason } => {
                return MissingLocalResume::Undecided(reason)
            }
        };

        match self.restore_claimed_sandbox(*entry, timeout).await {
            CrossNodeResume::Restored(metadata) => MissingLocalResume::Resumed(metadata),
            CrossNodeResume::NotFound => MissingLocalResume::Unknown,
            CrossNodeResume::Failed(reason) => MissingLocalResume::Failed(reason),
        }
    }

    /// Rebuilds a claimed sandbox whose origin cannot serve its capture.
    ///
    /// The stale local record is dropped first: it names a capture no reopen can
    /// reach, and the rebuild writes a new one. The held claim is the mutual
    /// exclusion against the origin coming back and resuming the same sandbox.
    pub(in crate::api) async fn rebuild_instead_of_reopening(
        &self,
        entry: Box<PausedSandboxEntry>,
        timeout: NewTimeout,
    ) -> CrossNodeResume {
        let sandbox_id = entry.sandbox_id;
        let generation = entry.generation;

        info!(
            %sandbox_id,
            origin_node_id = %entry.origin_node_id,
            "the node this paused sandbox names cannot serve its capture; rebuilding it from \
             the published snapshot"
        );

        match self
            .orchestrator()
            .discard_local_paused_record(sandbox_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.release_claim(&sandbox_id, generation).await;

                return CrossNodeResume::Failed(
                    "the paused record left its parked state before it could be rebuilt"
                        .to_string(),
                );
            }
            Err(err) => {
                self.release_claim(&sandbox_id, generation).await;

                return CrossNodeResume::Failed(format!(
                    "failed to drop the paused record whose capture is gone: {err}"
                ));
            }
        }

        self.restore_claimed_sandbox(*entry, timeout).await
    }

    /// Rebuilds a sandbox from a claim already held by this node.
    pub async fn restore_claimed_sandbox(
        &self,
        entry: PausedSandboxEntry,
        timeout: NewTimeout,
    ) -> CrossNodeResume {
        let sandbox_id = entry.sandbox_id;

        // A granted claim must name the snapshot to rebuild.
        let Some(snapshot_id) = entry.snapshot_id.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed("paused sandbox has no published snapshot".to_string());
        };

        // Missing metadata cannot be safely reconstructed from defaults.
        let Some(metadata) = entry.metadata.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed(
                "paused sandbox claim carries no sandbox record".to_string(),
            );
        };

        info!(
            %sandbox_id,
            %snapshot_id,
            origin_node_id = %entry.origin_node_id,
            "restoring paused sandbox from another node's snapshot"
        );

        // Read at any status so an unfinished row is not mistaken for destructive absence.
        let record = match self
            .snapshot_manager
            .get_scoped(snapshot_id.to_string(), CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) if record.committed.is_some() => record,
            Ok(Some(_)) => {
                // The row exists but is not yet runnable; preserve the registry claim.
                warn!(
                    %sandbox_id,
                    %snapshot_id,
                    "paused snapshot has a catalog row but no committed payload; leaving the \
                     registry row alone and failing this resume"
                );
                self.release_claim(&sandbox_id, entry.generation).await;

                return CrossNodeResume::Failed(
                    "paused snapshot has not finished publishing".to_string(),
                );
            }
            Ok(None) => {
                // Destructive absence requires agreement beyond one read-side lookup.
                match self.snapshot_manager.absence_of(&snapshot_id).await {
                    Ok(SnapshotAbsence::Settled) => {}
                    Ok(SnapshotAbsence::Unsettled { because }) => {
                        warn!(
                            %sandbox_id,
                            %snapshot_id,
                            because,
                            "the read side does not hold this paused snapshot but its absence is \
                             contradicted; leaving the registry row alone and failing this resume"
                        );
                        self.release_claim(&sandbox_id, entry.generation).await;

                        return CrossNodeResume::Failed(
                            "paused snapshot has not reached this node's catalog yet".to_string(),
                        );
                    }
                    Err(err) => {
                        // An unanswered existence query is not absence.
                        warn!(
                            error = ?err,
                            %sandbox_id,
                            %snapshot_id,
                            "could not settle whether this paused snapshot is really gone; \
                             leaving the registry row alone and failing this resume"
                        );
                        self.release_claim(&sandbox_id, entry.generation).await;

                        return CrossNodeResume::Failed(format!(
                            "failed to settle whether the paused snapshot still exists: {err}"
                        ));
                    }
                }

                warn!(%sandbox_id, %snapshot_id, "paused snapshot is missing from the repository");
                // Remove only the exact claimed generation whose snapshot is settled absent.
                match self
                    .paused
                    .registry()
                    .remove(&sandbox_id, entry.generation)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => warn!(
                        %sandbox_id,
                        "the dangling registry row changed hands before it could be dropped"
                    ),
                    Err(err) => {
                        warn!(error = %err, %sandbox_id, "failed to drop the dangling registry row")
                    }
                }

                return CrossNodeResume::NotFound;
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, %snapshot_id, "failed to load paused snapshot");
                self.release_claim(&sandbox_id, entry.generation).await;

                return CrossNodeResume::Failed(format!("failed to load paused snapshot: {err}"));
            }
        };

        // Remote orchestration needs the catalog row, not node-local runnable artifacts.
        let source = SandboxLaunchSource::SnapshotRecord(Box::new(record));

        // Run under the incarnation the claim allocated; the row is fenced on it.
        let request = restore_request(&metadata, source, timeout, entry.execution_id);

        match self
            .orchestrator()
            .restore_sandbox(sandbox_id, request)
            .await
        {
            Ok(metadata) => {
                // Repoint cluster ownership to the machine that actually accepted the restore.
                let holding_node_id = self
                    .orchestrator()
                    .sandbox_holding_node_id(&sandbox_id)
                    .await;
                self.paused
                    .mark_sandbox_running(
                        sandbox_id,
                        metadata.execution_id,
                        metadata.expires_at,
                        holding_node_id,
                    )
                    .await;

                CrossNodeResume::Restored(Box::new(metadata))
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, "failed to restore paused sandbox");
                self.release_claim(&sandbox_id, entry.generation).await;

                CrossNodeResume::Failed(err.to_string())
            }
        }
    }

    /// Renews leases for every local sandbox row held by this node.
    ///
    /// The registry decides which submitted rows this node actually owns.
    pub async fn renew_paused_leases(&self) {
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        let sandboxes = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter::default())
            .await
        {
            Ok(sandboxes) => sandboxes,
            Err(err) => {
                warn!(error = ?err, "failed to list local sandboxes for lease renewal");

                return;
            }
        };

        // Use the live deadline; paused-row metadata may be stale after timeout changes.
        let held: Vec<HeldSandbox> = sandboxes
            .into_iter()
            .map(|metadata| HeldSandbox {
                sandbox_id: metadata.id,
                expires_at: metadata.expires_at.map(DateTime::<Utc>::from),
            })
            .collect();
        match self
            .paused
            .registry()
            .renew_lease(self.paused.node_id(), &held)
            .await
        {
            Ok(_) => self.paused.observe_lease_renewal(true),
            Err(err) => {
                self.paused.observe_lease_renewal(false);
                warn!(
                    error = %err,
                    "failed to renew paused registry leases; sandboxes parked here with an \
                     unpublished snapshot may be rebuilt elsewhere from an older one"
                );
            }
        }
    }

    /// Releases holdings left by the previous process on this machine.
    ///
    /// Startup must call this before the process can put its identity on a live row.
    pub async fn release_stale_node_holdings(&self) -> StaleReleaseOutcome {
        if !self.paused.registry().is_cluster_backed() {
            return StaleReleaseOutcome::Released;
        }

        self.paused.release_stale_holdings().await
    }

    /// Retries startup holding release only while this process has taken nothing live.
    ///
    /// Once fenced, releasing by node identity is unsafe and retries stop.
    pub async fn retry_stale_node_holdings_release(&self) {
        loop {
            tokio::time::sleep(Self::STALE_RELEASE_RETRY_INTERVAL).await;

            match self.release_stale_node_holdings().await {
                StaleReleaseOutcome::Released => {
                    info!(
                        node_id = self.paused.node_id(),
                        "released the previous process's holdings on a retry"
                    );

                    return;
                }
                StaleReleaseOutcome::Fenced => {
                    metrics::counter!("agentenv_paused_registry_stale_release_abandoned_total")
                        .increment(1);
                    error!(
                        node_id = self.paused.node_id(),
                        "gave up releasing the previous process's holdings: this node now runs \
                         sandboxes of its own, so releasing by node identity would give one of \
                         them away. Any sandbox the previous process was running stays \
                         unclaimable until this node restarts"
                    );

                    return;
                }
                // Logged and counted at the point of failure.
                StaleReleaseOutcome::Failed => {}
            }
        }
    }

    /// Reclaims expired rows from nodes that stopped reporting.
    ///
    /// The cluster operation is conditional and safe for multiple concurrent callers.
    pub async fn reclaim_expired_sandboxes(&self) {
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        match self.paused.registry().reclaim_expired_holdings().await {
            Ok(reclaimed) if reclaimed.is_empty() => {}
            Ok(reclaimed) => {
                metrics::counter!("agentenv_paused_registry_expired_reclaimed_total")
                    .increment(reclaimed.released);
                metrics::counter!("agentenv_paused_registry_expired_discarded_total")
                    .increment(reclaimed.discarded);
                info!(
                    released = reclaimed.released,
                    discarded = reclaimed.discarded,
                    "reclaimed expired sandboxes from nodes that stopped reporting"
                );
            }
            Err(err) => {
                warn!(error = %err, "failed to reclaim expired sandboxes");
            }
        }
    }

    /// Deletes cluster state and snapshot for a sandbox confirmed gone.
    pub async fn forget_paused_sandbox(&self, sandbox_id: SandboxId) {
        // With no local handle, a live row belonging to another holder must remain untouched.
        self.paused.forget_sandbox(sandbox_id, None).await;
    }

    /// Reconciles local paused and running copies against cluster ownership.
    ///
    /// Runs periodically because a partitioned node receives no takeover notification.
    pub async fn reconcile_local_records(&self) {
        // A disabled registry would make every local record appear missing.
        if !self.paused.registry().is_cluster_backed() {
            return;
        }

        self.retain_running_registrations().await;
        self.reconcile_local_paused_records().await;
        self.reap_superseded_running_sandboxes().await;
    }

    /// Prunes registrations for sandboxes no longer in the local roster.
    async fn retain_running_registrations(&self) {
        match self.orchestrator.list_sandbox_ids().await {
            Ok(ids) => self
                .paused
                .retain_running_registrations(&ids.into_iter().collect()),
            Err(err) => {
                warn!(error = ?err, "failed to list local sandboxes to prune registrations")
            }
        }
    }

    /// Tears down registered running copies whose cluster row names another owner.
    async fn reap_superseded_running_sandboxes(&self) {
        let running = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter {
                states: Some(vec![crate::orchestrator::SandboxState::Running]),
                ..Default::default()
            })
            .await
        {
            Ok(running) => running,
            Err(err) => {
                warn!(error = ?err, "failed to list local running sandboxes for reconciliation");

                return;
            }
        };

        // Query only sandboxes with a confirmed cluster registration.
        let registered: Vec<(SandboxId, String)> = running
            .into_iter()
            .filter_map(|metadata| {
                self.paused
                    .running_registration(&metadata.id)
                    .map(|registered_as| (metadata.id, registered_as))
            })
            .collect();
        if registered.is_empty() {
            return;
        }

        let ids: Vec<SandboxId> = registered.iter().map(|(id, _)| *id).collect();
        let rows = match self.paused.registry().get_many(&ids).await {
            Ok(rows) => rows,
            Err(err) => {
                // An unreadable batch cannot authorize any teardown.
                warn!(error = %err, "registry unreadable; stopping running-sandbox reconciliation");

                return;
            }
        };
        // Partial batch answers leave unanswered sandboxes untouched.
        let answered = rows.answered();
        if !rows.covers(&ids) {
            warn!(
                requested = ids.len(),
                covered = answered.len(),
                "registry answered for only part of the roster; leaving the rest of the running sandboxes alone"
            );
        }

        for (sandbox_id, registered_as) in registered {
            // Unanswered ids remain undecided.
            let Some(row) = answered.get(&sandbox_id) else {
                continue;
            };
            let Some(superseded) = running_supersession(row, &registered_as, self.paused.node_id())
            else {
                continue;
            };

            warn!(
                %sandbox_id,
                reason = %superseded.reason(),
                "tearing down a running sandbox the cluster holds elsewhere"
            );

            match self
                .orchestrator()
                .discard_superseded_sandbox(sandbox_id)
                .await
            {
                Ok(()) => {
                    record_supersession("running", "discarded");
                    info!(%sandbox_id, "discarded superseded running sandbox");
                }
                Err(err) => {
                    record_supersession("running", "failed");
                    warn!(error = ?err, %sandbox_id, "failed to discard superseded running sandbox")
                }
            }
        }
    }

    /// Drops local paused records superseded by cluster ownership.
    async fn reconcile_local_paused_records(&self) {
        let paused = match self
            .orchestrator
            .list_sandboxes_filtered(SandboxListFilter {
                states: Some(vec![crate::orchestrator::SandboxState::Paused]),
                ..Default::default()
            })
            .await
        {
            Ok(paused) => paused,
            Err(err) => {
                warn!(error = ?err, "failed to list local paused sandboxes for reconciliation");

                return;
            }
        };

        // Registry silence says nothing about records never registered.
        let mut registered = Vec::with_capacity(paused.len());
        for metadata in paused {
            match self
                .orchestrator
                .paused_record_cluster_registration(metadata.id)
                .await
            {
                Ok(ClusterRegistration::Never) => continue,
                Ok(registration) => registered.push((metadata.id, registration)),
                Err(err) => {
                    warn!(error = ?err, sandbox_id = %metadata.id, "failed to read local registration marker")
                }
            }
        }
        if registered.is_empty() {
            return;
        }

        let ids: Vec<SandboxId> = registered.iter().map(|(id, _)| *id).collect();
        let rows = match self.paused.registry().get_many(&ids).await {
            Ok(rows) => rows,
            Err(err) => {
                // An unreadable response cannot authorize local deletion.
                warn!(error = %err, "registry unreadable; stopping paused-record reconciliation");

                return;
            }
        };

        // Partial batch answers leave unanswered local records untouched.
        let answered = rows.answered();
        if !rows.covers(&ids) {
            warn!(
                requested = ids.len(),
                covered = answered.len(),
                "registry answered for only part of the roster; leaving the rest of the paused records alone"
            );
        }

        let mut discarded = 0usize;
        for (sandbox_id, registration) in registered {
            // Outer absence means unanswered; an answered missing row means gone.
            let Some(row) = answered.get(&sandbox_id) else {
                continue;
            };
            let superseded = match row {
                None => Superseded::Gone,
                Some(entry) => match supersession(entry, &registration) {
                    Some(superseded) => superseded,
                    None => continue,
                },
            };

            match self
                .orchestrator()
                .discard_local_paused_record(sandbox_id)
                .await
            {
                Ok(true) => {
                    record_supersession("paused", "discarded");
                    info!(%sandbox_id, reason = %superseded.reason(), "discarded superseded paused record");
                    discarded += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    record_supersession("paused", "failed");
                    warn!(error = ?err, %sandbox_id, "failed to discard stranded paused record")
                }
            }
        }

        if discarded > 0 {
            info!(
                discarded,
                "discarded paused records the cluster has moved past"
            );
        }
    }

    /// Discards a local paused copy only when cluster state confirms supersession.
    ///
    /// Registry uncertainty preserves the local record.
    pub async fn discard_if_superseded(&self, sandbox_id: SandboxId) -> bool {
        if !self.paused.registry().is_cluster_backed() {
            return false;
        }

        let superseded = match self.superseded_by_cluster(sandbox_id).await {
            Ok(Some(superseded)) => superseded,
            Ok(None) => return false,
            // Log registry uncertainty distinctly from an owned row.
            Err(()) => {
                debug!(
                    %sandbox_id,
                    "the registry could not say whether this node's paused copy has been \
                     superseded; leaving it in place"
                );

                return false;
            }
        };

        match self
            .orchestrator()
            .discard_local_paused_record(sandbox_id)
            .await
        {
            Ok(true) => {
                info!(%sandbox_id, reason = %superseded.reason(), "discarded superseded paused record");

                true
            }
            _ => false,
        }
    }

    /// Determines whether cluster state supersedes this node's paused copy.
    async fn superseded_by_cluster(&self, sandbox_id: SandboxId) -> Result<Option<Superseded>, ()> {
        // Never judge a local copy the cluster was not told about.
        let registration = match self
            .orchestrator
            .paused_record_cluster_registration(sandbox_id)
            .await
        {
            Ok(ClusterRegistration::Never) => return Ok(None),
            Ok(registration) => registration,
            Err(err) => {
                warn!(error = ?err, %sandbox_id, "failed to read local registration marker");

                return Ok(None);
            }
        };

        let entry = match self.paused.registry().get(&sandbox_id).await {
            Ok(entry) => entry,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to read the paused registry row");

                return Err(());
            }
        };

        let Some(entry) = entry else {
            return Ok(Some(Superseded::Gone));
        };

        Ok(supersession(&entry, &registration))
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) {
        if let Err(err) = self
            .paused
            .registry()
            .release_claim(sandbox_id, generation)
            .await
        {
            warn!(
                error = %err,
                %sandbox_id,
                "failed to release the resume claim; the sandbox stays marked as resuming"
            );
        }
    }
}

/// Chooses whether a registry outage may fall back to local resume.
///
/// Only records never announced under a known cluster identity may proceed;
/// announced records fail closed to avoid a second live copy.
fn unreachable_arbitration(
    registration: Option<ClusterRegistration>,
    sandbox_id: SandboxId,
    reason: &str,
    proposed: ExecutionId,
) -> ResumeArbitration {
    if !matches!(registration, Some(ClusterRegistration::As(_))) {
        metrics::counter!(
            "agentenv_paused_registry_resume_unarbitrated_total",
            "outcome" => "proceeded",
        )
        .increment(1);
        warn!(
            error = %reason,
            %sandbox_id,
            "could not reach the registry to arbitrate a resume; proceeding locally because \
             this node holds no copy the cluster was told about"
        );

        return ResumeArbitration::Proceed(ClaimedExecution::from_claim(proposed));
    }

    metrics::counter!(
        "agentenv_paused_registry_resume_unarbitrated_total",
        "outcome" => "refused",
    )
    .increment(1);
    warn!(
        error = %reason,
        %sandbox_id,
        "could not reach the registry to arbitrate a resume for a sandbox this node announced \
         to the cluster; refusing rather than risking a second live copy"
    );

    ResumeArbitration::Unavailable {
        reason: reason.to_string(),
    }
}

/// Converts one registry claim result into this node's resume decision.
fn arbitration(claim: ResumeClaim, node_id: &str, proposed: ExecutionId) -> ResumeArbitration {
    match claim {
        ResumeClaim::Claimed { entry, .. } => {
            // Prefer the incarnation written by the registry, falling back for old rows.
            let granted = entry.execution_id.unwrap_or(proposed);
            ResumeArbitration::Held(entry, ClaimedExecution::from_claim(granted))
        }
        // Untracked sandboxes have no cluster owner to arbitrate with.
        ResumeClaim::NotFound => ResumeArbitration::Proceed(ClaimedExecution::from_claim(proposed)),
        ResumeClaim::NotReady { origin_node_id } if origin_node_id == node_id => {
            ResumeArbitration::Proceed(ClaimedExecution::from_claim(proposed))
        }
        ResumeClaim::Conflict {
            origin_node_id,
            reason: _,
        } if origin_node_id == node_id => {
            ResumeArbitration::Proceed(ClaimedExecution::from_claim(proposed))
        }
        ResumeClaim::NotReady { origin_node_id } => ResumeArbitration::NotReady { origin_node_id },
        // Both conflict reasons block this attempt; callers decide whether to retry.
        ResumeClaim::Conflict {
            origin_node_id,
            reason: _,
        } => ResumeArbitration::Blocked { origin_node_id },
    }
}

/// Determines whether cluster ownership supersedes a local paused copy.
///
/// Compare with the identity recorded at registration, not the node's current id.
fn supersession(
    entry: &PausedSandboxEntry,
    registration: &ClusterRegistration,
) -> Option<Superseded> {
    // Live or resuming rows supersede a paused local copy.
    match entry.state {
        PausedRegistryState::Running => {
            return Some(Superseded::HeldBy(entry.origin_node_id.clone()))
        }
        PausedRegistryState::Resuming => {
            return Some(Superseded::ClaimedBy(
                entry
                    .claimed_by_node_id
                    .clone()
                    .unwrap_or_else(|| entry.origin_node_id.clone()),
            ))
        }
        _ => {}
    }

    // A parked row under another registered holder supersedes this copy.
    match registration {
        ClusterRegistration::As(node_id) if entry.origin_node_id != *node_id => {
            Some(Superseded::HeldBy(entry.origin_node_id.clone()))
        }
        _ => None,
    }
}

/// Records paused/running supersession outcomes.
fn record_supersession(kind: &'static str, outcome: &'static str) {
    metrics::counter!(
        "agentenv_paused_registry_superseded_total",
        "kind" => kind,
        "outcome" => outcome,
    )
    .increment(1);
}

/// Determines whether cluster ownership supersedes a registered running copy.
///
/// Holder identity and claimant identity are distinct; resuming rows compare
/// their claimant against this process, not against the machine holder.
fn running_supersession(
    entry: Option<&PausedSandboxEntry>,
    registered_as: &str,
    claimant_node_id: &str,
) -> Option<Superseded> {
    let Some(entry) = entry else {
        return Some(Superseded::Gone);
    };

    match entry.state {
        // Stable states are owned by the row's holder.
        PausedRegistryState::Running
        | PausedRegistryState::Paused
        | PausedRegistryState::Publishing
        | PausedRegistryState::LocalOnly => (entry.origin_node_id != registered_as)
            .then(|| Superseded::HeldBy(entry.origin_node_id.clone())),
        // Resuming rows are owned by their claimant, not their artifact holder.
        PausedRegistryState::Resuming => {
            let claimer = entry
                .claimed_by_node_id
                .clone()
                .unwrap_or_else(|| entry.origin_node_id.clone());

            (claimer != claimant_node_id).then_some(Superseded::ClaimedBy(claimer))
        }
    }
}

/// Builds a restore request from persisted metadata and the selected launch source.
fn restore_request(
    metadata: &SandboxMetadata,
    source: SandboxLaunchSource,
    timeout: NewTimeout,
    run_as: Option<ExecutionId>,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source,
        // Existing absence of a deadline remains absence; never invent a default.
        expiry: match timeout {
            NewTimeout::Set(duration) | NewTimeout::EnsureMinimum(duration) => {
                SandboxExpiry::After(duration)
            }
            NewTimeout::UseExisting => match metadata.timeout {
                Some(duration) => SandboxExpiry::After(duration),
                None => SandboxExpiry::NotKeptHere,
            },
            NewTimeout::None => SandboxExpiry::NotKeptHere,
        },
        timeout_action: metadata.timeout_action,
        auto_resume: metadata.auto_resume,
        user_metadata: metadata.user_metadata.clone(),
        env_vars: None,
        network_policy: metadata.network_policy.clone(),
        secure: metadata.secure,
        custom_extension_params: metadata.custom_extension_params.clone(),
        // Preserve the control-plane owner across runs.
        control_plane_config: metadata.control_plane_config.clone(),
        // The claim already allocated this restore's incarnation, and the row is
        // fenced on it: minting a second one makes `mark_running` match no row and
        // strands the claim in `resuming`.
        execution_id: run_as,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::super::paused_coordinator::test_support::CountingRegistry;
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, DisabledSandboxPersister, FileBackedSandboxPersister,
        InMemoryMetadataStore, Orchestrator, PausedSandboxRegistry,
    };
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::mock_snapshot_manager;
    use crate::snapshot::SnapshotId;

    const SELF: &str = "node-a";
    const OTHER: &str = "node-b";
    /// Machine holder distinct from both claimant identities.
    const HOLDER: &str = "aenv-worker-07";

    /// Minimal API fixture for startup release behavior.
    async fn api_over(registry: Arc<dyn PausedSandboxRegistry>) -> Arc<ApiImpl> {
        let root = tempfile::tempdir().unwrap();

        api_rooted(root.path(), registry).await
    }

    /// Fixture rooted in a caller-owned record store.
    ///
    /// Seed it before this call: the orchestrator loads records as it starts.
    async fn api_rooted(
        root: &std::path::Path,
        registry: Arc<dyn PausedSandboxRegistry>,
    ) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.to_path_buf()),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .unwrap();
        let snapshot_manager = Arc::new(mock_snapshot_manager());

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::node_local(NodeIdentity::from_config(&Default::default()).id),
        ))
    }

    /// Seeds a persisted paused record that startup retains for inspection.
    async fn seed_paused_record(
        root: &std::path::Path,
        registered_as: Option<&str>,
    ) -> crate::types::SandboxId {
        use crate::orchestrator::SandboxPersister;

        let persister = FileBackedSandboxPersister::new_for_test(root.to_path_buf());
        let sandbox_id = crate::types::SandboxId::new();
        let artifacts = root.join("artifacts").join(sandbox_id.to_string());
        std::fs::create_dir_all(&artifacts).unwrap();
        let paused_state: Arc<dyn crate::sandbox::PausedSandboxState> =
            Arc::new(crate::sandbox::mock::MockSnapshot);

        persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    virtualization_mode: crate::virtualization::VirtualizationMode::Pvm,
                    ..Default::default()
                },
                Some(&artifacts),
                paused_state.as_ref(),
            )
            .await
            .unwrap();
        if let Some(node_id) = registered_as {
            persister
                .mark_cluster_registered(&sandbox_id, node_id)
                .await
                .unwrap();
        }

        sandbox_id
    }

    /// Metadata-store fixture that reports one sandbox as remote without a live handle.
    struct RemoteOriginStore {
        inner: InMemoryMetadataStore,
        remote: std::sync::Mutex<Option<(SandboxId, Option<String>)>>,
    }

    impl RemoteOriginStore {
        fn new() -> Self {
            Self {
                inner: InMemoryMetadataStore::new(),
                remote: std::sync::Mutex::new(None),
            }
        }

        /// Makes `paused_handle(sandbox_id)` answer `Remote { origin_node_id, .. }`.
        fn name_remote_origin(&self, sandbox_id: SandboxId, origin_node_id: Option<&str>) {
            *self.remote.lock().unwrap() = Some((sandbox_id, origin_node_id.map(str::to_string)));
        }
    }

    #[async_trait::async_trait]
    impl crate::orchestrator::MetadataStore for RemoteOriginStore {
        async fn add(
            &self,
            metadata: SandboxMetadata,
        ) -> std::result::Result<(), crate::orchestrator::StoreError> {
            self.inner.add(metadata).await
        }

        async fn update(
            &self,
            metadata: SandboxMetadata,
        ) -> std::result::Result<(), crate::orchestrator::StoreError> {
            self.inner.update(metadata).await
        }

        async fn update_state_if_state(
            &self,
            sandbox_id: &SandboxId,
            new_state: crate::orchestrator::SandboxState,
            expected_states: &[crate::orchestrator::SandboxState],
        ) -> std::result::Result<crate::orchestrator::SandboxState, crate::orchestrator::StoreError>
        {
            self.inner
                .update_state_if_state(sandbox_id, new_state, expected_states)
                .await
        }

        async fn update_if_state<F>(
            &self,
            sandbox_id: &SandboxId,
            expected_states: &[crate::orchestrator::SandboxState],
            update: F,
        ) -> std::result::Result<
            crate::orchestrator::MetadataUpdateResult,
            crate::orchestrator::StoreError,
        >
        where
            F: FnOnce(&mut SandboxMetadata) + Send,
        {
            self.inner
                .update_if_state(sandbox_id, expected_states, update)
                .await
        }

        async fn get(
            &self,
            sandbox_id: &SandboxId,
        ) -> std::result::Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner.get(sandbox_id).await
        }

        async fn remove(
            &self,
            sandbox_id: &SandboxId,
        ) -> std::result::Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner.remove(sandbox_id).await
        }

        async fn remove_if_execution(
            &self,
            sandbox_id: &SandboxId,
            expected_execution_id: crate::types::ExecutionId,
            expected_states: &[crate::orchestrator::SandboxState],
        ) -> std::result::Result<crate::orchestrator::FencedRemoval, crate::orchestrator::StoreError>
        {
            self.inner
                .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
                .await
        }

        async fn list(
            &self,
        ) -> std::result::Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner.list().await
        }

        async fn list_with_callback<F>(
            &self,
            callback: F,
        ) -> std::result::Result<(), crate::orchestrator::StoreError>
        where
            F: FnMut(&SandboxMetadata) + Send,
        {
            self.inner.list_with_callback(callback).await
        }

        async fn list_filtered(
            &self,
            filter: crate::orchestrator::SandboxListFilter,
        ) -> std::result::Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner.list_filtered(filter).await
        }

        async fn list_expired(
            &self,
            now: std::time::SystemTime,
        ) -> std::result::Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner.list_expired(now).await
        }

        async fn list_ids(
            &self,
        ) -> std::result::Result<Vec<SandboxId>, crate::orchestrator::StoreError> {
            self.inner.list_ids().await
        }

        async fn wait_while_in_states(
            &self,
            sandbox_id: &SandboxId,
            transitional_states: &[crate::orchestrator::SandboxState],
        ) -> std::result::Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
            self.inner
                .wait_while_in_states(sandbox_id, transitional_states)
                .await
        }

        async fn paused_handle(
            &self,
            sandbox_id: &SandboxId,
        ) -> std::result::Result<crate::orchestrator::PausedHandle, crate::orchestrator::StoreError>
        {
            if let Some((remote_id, origin_node_id)) = self.remote.lock().unwrap().clone() {
                if remote_id == *sandbox_id {
                    return Ok(crate::orchestrator::PausedHandle::Remote {
                        reference: crate::orchestrator::PausedStateRef {
                            artifact_root: None,
                            state: serde_json::Value::Null,
                        },
                        origin_node_id,
                    });
                }
            }

            Ok(crate::orchestrator::PausedHandle::NotPaused)
        }
    }

    async fn api_with_remote_origin(
        store: RemoteOriginStore,
        registry: Arc<CountingRegistry>,
    ) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            store,
            MockBackendFactory::new(),
            DisabledSandboxPersister,
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .unwrap();
        let snapshot_manager = Arc::new(mock_snapshot_manager());

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    #[tokio::test]
    async fn two_replicas_reading_the_same_origin_hint_do_not_mistake_each_others_claim_for_their_own(
    ) {
        let store = RemoteOriginStore::new();
        let sandbox_id = SandboxId::new();
        store.name_remote_origin(sandbox_id, Some("node-203"));
        let registry = Arc::new(CountingRegistry::answering_conflict("node-203"));
        let api = api_with_remote_origin(store, Arc::clone(&registry)).await;

        let arbitration = api.arbitrate_resume(sandbox_id).await;

        assert!(
            matches!(arbitration, ResumeArbitration::Blocked { .. }),
            "a conflict naming the shared origin hint must block this replica, not read as \
             its own claim — proceeding here is the double-live bug"
        );
        assert_eq!(
            registry.claimed_as(),
            vec![api.paused.node_id().to_string()],
            "the claim itself must be taken under this process's own identity, never the \
             shared origin hint — quoting the hint here is what makes the misjudgement above \
             possible in the first place"
        );
        assert_ne!(
            registry.claimed_as(),
            vec!["node-203".to_string()],
            "control: the claimant must not equal the shared hint this store also answers to \
             every other replica"
        );
    }

    #[tokio::test]
    async fn a_claim_is_always_taken_under_this_process_never_the_shared_origin_hint() {
        let store = RemoteOriginStore::new();
        let named = SandboxId::new();
        let unnamed = SandboxId::new();
        store.name_remote_origin(named, Some("node-203"));
        let registry = Arc::new(CountingRegistry::new(0, false));
        let api = api_with_remote_origin(store, Arc::clone(&registry)).await;

        api.arbitrate_resume(named).await;
        api.arbitrate_resume(unnamed).await;

        assert_eq!(
            registry.claimed_as(),
            vec![
                api.paused.node_id().to_string(),
                api.paused.node_id().to_string(),
            ],
            "both claims — one for a sandbox whose real machine is on record, one for a \
             sandbox nobody has heard of — must name this process, and only this process"
        );
    }

    #[tokio::test]
    async fn a_conflict_naming_this_process_own_identity_is_not_a_refusal() {
        // One throwaway instance just to learn this test's own node identity —
        // deterministic (hostname-derived, see NodeIdentity::from_config), so
        // reusing it to program the registry below is exact, not a guess.
        let self_id = api_with_remote_origin(
            RemoteOriginStore::new(),
            Arc::new(CountingRegistry::new(0, false)),
        )
        .await
        .paused
        .node_id()
        .to_string();

        let store = RemoteOriginStore::new();
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(CountingRegistry::answering_conflict(&self_id));
        let api = api_with_remote_origin(store, Arc::clone(&registry)).await;
        assert_eq!(
            api.paused.node_id(),
            self_id,
            "sanity: same process identity"
        );

        let arbitration = api.arbitrate_resume(sandbox_id).await;

        assert!(
            matches!(arbitration, ResumeArbitration::Proceed(_)),
            "a conflict naming this process's own identity is this process's own claim, not \
             a refusal"
        );
    }

    #[tokio::test]
    async fn an_unreachable_registry_refuses_a_resume_for_a_copy_the_cluster_knows_about() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;
        let recorder = crate::logging::capture::Recorder::default();
        let _guard = recorder.install();

        assert!(matches!(
            api.arbitrate_resume(sandbox_id).await,
            ResumeArbitration::Unavailable { .. }
        ));
        assert!(
            recorder.saw(tracing::Level::WARN, "refusing rather than risking"),
            "the refusal has to be findable: {:?}",
            recorder.events()
        );
    }

    #[tokio::test]
    async fn a_resume_nobody_could_arbitrate_is_retryable_not_a_missing_sandbox() {
        use agentenv_http_server::apis::sandboxes::{
            Sandboxes, SandboxesSandboxIdResumePostResponse,
        };
        use agentenv_http_server::models;

        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;

        let answer = api
            .sandboxes_sandbox_id_resume_post(
                &http::Method::POST,
                &headers::Host::from(http::uri::Authority::from_static("localhost")),
                &axum_extra::extract::CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdResumePostPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
                &models::ResumedSandbox::new(),
            )
            .await
            .expect("the handler answers rather than failing the request");

        let SandboxesSandboxIdResumePostResponse::Status500_ServerError(error) = answer else {
            panic!("an unarbitrated resume must be retryable, got {answer:?}");
        };
        assert!(
            error
                .message
                .contains("cannot determine whether the sandbox is live elsewhere"),
            "the refusal has to say what happened: {error:?}"
        );
    }

    #[tokio::test]
    async fn an_unreachable_registry_still_proceeds_for_a_copy_the_cluster_never_saw() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), None).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;

        // The record has to still be there, or this passes for the wrong
        // reason — a store that discarded it answers the same way.
        assert_eq!(
            api.orchestrator()
                .paused_record_cluster_registration(sandbox_id)
                .await
                .expect("the seeded record should have survived startup"),
            ClusterRegistration::Never
        );
        assert!(matches!(
            api.arbitrate_resume(sandbox_id).await,
            ResumeArbitration::Proceed(_)
        ));
    }

    #[tokio::test]
    async fn an_unreachable_registry_still_proceeds_when_this_node_holds_nothing() {
        let api = api_over(Arc::new(CountingRegistry::unreachable())).await;

        assert!(matches!(
            api.arbitrate_resume(crate::types::SandboxId::new()).await,
            ResumeArbitration::Proceed(_)
        ));
    }

    #[tokio::test]
    async fn a_node_local_registry_never_refuses_a_resume() {
        let api = api_over(Arc::new(DisabledPausedSandboxRegistry)).await;

        assert!(matches!(
            api.arbitrate_resume(crate::types::SandboxId::new()).await,
            ResumeArbitration::Proceed(_)
        ));
    }

    #[test]
    fn only_a_copy_announced_under_a_known_identity_refuses_a_resume() {
        let sandbox_id = crate::types::SandboxId::new();

        assert!(matches!(
            unreachable_arbitration(
                Some(ClusterRegistration::As(SELF.to_string())),
                sandbox_id,
                "no route to host",
                ExecutionId::new(),
            ),
            ResumeArbitration::Unavailable { .. }
        ));
        // Never announced: the local copy is the only copy.
        assert!(matches!(
            unreachable_arbitration(
                Some(ClusterRegistration::Never),
                sandbox_id,
                "no route",
                ExecutionId::new()
            ),
            ResumeArbitration::Proceed(_)
        ));
        // Announced by a build that did not store the identity: the row cannot
        // be judged against it either, which is how `supersession` treats it.
        assert!(matches!(
            unreachable_arbitration(
                Some(ClusterRegistration::Anonymous),
                sandbox_id,
                "no route",
                ExecutionId::new()
            ),
            ResumeArbitration::Proceed(_)
        ));
        // No local record at all: nothing here to duplicate.
        assert!(matches!(
            unreachable_arbitration(None, sandbox_id, "no route", ExecutionId::new()),
            ResumeArbitration::Proceed(_)
        ));
    }

    #[tokio::test]
    async fn a_registry_that_cannot_be_asked_says_so_before_keeping_the_local_copy() {
        let root = tempfile::tempdir().unwrap();
        let sandbox_id = seed_paused_record(root.path(), Some(SELF)).await;
        let api = api_rooted(root.path(), Arc::new(CountingRegistry::unreachable())).await;
        let recorder = crate::logging::capture::Recorder::default();
        let _guard = recorder.install();

        assert!(!api.discard_if_superseded(sandbox_id).await);
        assert!(
            recorder.saw(tracing::Level::DEBUG, "leaving it in place"),
            "the non-deletion has to be findable: {:?}",
            recorder.events()
        );
    }

    #[tokio::test]
    async fn consecutive_failed_renewals_are_counted() {
        let api = api_over(Arc::new(CountingRegistry::unreachable())).await;

        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 1);
        api.renew_paused_leases().await;
        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 3);
    }

    #[tokio::test]
    async fn a_renewal_that_lands_ends_the_run() {
        let api = api_over(Arc::new(CountingRegistry::unreachable_for(2))).await;

        api.renew_paused_leases().await;
        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 2);

        api.renew_paused_leases().await;
        assert_eq!(api.paused.consecutive_renew_failures(), 0);
    }

    #[tokio::test]
    async fn a_node_local_registry_has_nothing_to_release() {
        let api = api_over(Arc::new(DisabledPausedSandboxRegistry)).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Released
        );
    }

    /// Bounds a paused-time retry fixture so failures cannot spin indefinitely.
    async fn run_bounded(work: impl std::future::Future<Output = ()>) {
        tokio::time::timeout(Duration::from_secs(600), work)
            .await
            .expect("the retry loop should settle rather than run forever");
    }

    #[tokio::test(start_paused = true)]
    async fn the_retry_keeps_going_until_the_release_lands() {
        let registry = Arc::new(CountingRegistry::new(3, false));
        let api = api_over(Arc::clone(&registry) as Arc<dyn PausedSandboxRegistry>).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Failed
        );

        run_bounded(api.retry_stale_node_holdings_release()).await;

        assert_eq!(
            registry.release_calls(),
            4,
            "three failures then the one that landed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_retry_stops_once_this_node_holds_a_sandbox() {
        let registry = Arc::new(CountingRegistry::always_failing());
        let api = api_over(Arc::clone(&registry) as Arc<dyn PausedSandboxRegistry>).await;

        assert_eq!(
            api.release_stale_node_holdings().await,
            StaleReleaseOutcome::Failed
        );
        api.paused.note_taking_sandbox_live().await;

        run_bounded(api.retry_stale_node_holdings_release()).await;

        assert_eq!(
            registry.release_calls(),
            1,
            "only the startup attempt; the retry must not reach the registry"
        );
    }

    fn registered_as(node_id: &str) -> ClusterRegistration {
        ClusterRegistration::As(node_id.to_string())
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
    fn a_granted_claim_hands_back_the_registrys_incarnation() {
        let mut entry = entry(PausedRegistryState::Resuming, SELF, Some(SELF));
        let registry_side = ExecutionId::new();
        entry.execution_id = Some(registry_side);

        let ResumeArbitration::Held(_, claimed) = arbitration(
            ResumeClaim::Claimed {
                entry: Box::new(entry),
                previous_state: PausedRegistryState::Paused,
            },
            SELF,
            ExecutionId::new(),
        ) else {
            panic!("a granted claim is Held");
        };

        assert_eq!(claimed.execution_id(), registry_side);
    }

    #[test]
    fn a_claim_from_an_older_controller_runs_under_the_proposed_incarnation() {
        let mut entry = entry(PausedRegistryState::Resuming, SELF, Some(SELF));
        entry.execution_id = None;
        let proposed = ExecutionId::new();

        let ResumeArbitration::Held(_, claimed) = arbitration(
            ResumeClaim::Claimed {
                entry: Box::new(entry),
                previous_state: PausedRegistryState::Paused,
            },
            SELF,
            proposed,
        ) else {
            panic!("a granted claim is Held");
        };

        assert_eq!(claimed.execution_id(), proposed);
    }

    #[test]
    fn a_row_held_by_another_node_supersedes_the_local_copy() {
        let superseded = supersession(
            &entry(PausedRegistryState::Running, OTHER, None),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(node)) if node == OTHER));
    }

    #[test]
    fn a_paused_row_owned_by_another_node_also_supersedes() {
        let superseded = supersession(
            &entry(PausedRegistryState::Paused, OTHER, None),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(_))));
    }

    #[test]
    fn a_claim_by_another_node_supersedes_our_own_row() {
        let superseded = supersession(
            &entry(PausedRegistryState::Resuming, SELF, Some(OTHER)),
            &registered_as(SELF),
        );

        assert!(matches!(superseded, Some(Superseded::ClaimedBy(node)) if node == OTHER));
    }

    #[test]
    fn our_own_paused_row_is_not_superseded() {
        assert!(supersession(
            &entry(PausedRegistryState::Paused, SELF, None),
            &registered_as(SELF)
        )
        .is_none());
    }

    #[test]
    fn a_running_row_naming_another_node_supersedes_our_live_copy() {
        let superseded = running_supersession(
            Some(&entry(PausedRegistryState::Running, OTHER, None)),
            SELF,
            SELF,
        );

        assert!(matches!(superseded, Some(Superseded::HeldBy(node)) if node == OTHER));
    }

    #[test]
    fn a_parked_row_naming_another_node_supersedes_our_live_copy() {
        for state in [
            PausedRegistryState::Paused,
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
        ] {
            assert!(
                matches!(
                    running_supersession(Some(&entry(state, OTHER, None)), SELF, SELF),
                    Some(Superseded::HeldBy(_))
                ),
                "{state:?} on another node should supersede our running copy"
            );
        }
    }

    #[test]
    fn a_claim_by_another_node_supersedes_our_live_copy() {
        let superseded = running_supersession(
            Some(&entry(PausedRegistryState::Resuming, SELF, Some(OTHER))),
            SELF,
            SELF,
        );

        assert!(matches!(superseded, Some(Superseded::ClaimedBy(node)) if node == OTHER));
    }

    #[test]
    fn our_own_running_row_is_not_superseded() {
        assert!(running_supersession(
            Some(&entry(PausedRegistryState::Running, SELF, None)),
            SELF,
            SELF
        )
        .is_none());
    }

    #[test]
    fn our_own_claim_is_not_superseded() {
        assert!(running_supersession(
            Some(&entry(PausedRegistryState::Resuming, OTHER, Some(SELF))),
            SELF,
            SELF,
        )
        .is_none());
    }

    #[test]
    fn our_own_claim_is_not_superseded_even_when_holder_and_claimant_differ() {
        assert!(running_supersession(
            Some(&entry(PausedRegistryState::Resuming, OTHER, Some(SELF))),
            HOLDER,
            SELF,
        )
        .is_none());
    }

    #[test]
    fn a_claim_by_another_node_supersedes_our_live_copy_even_when_holder_and_claimant_differ() {
        let superseded = running_supersession(
            Some(&entry(PausedRegistryState::Resuming, SELF, Some(OTHER))),
            HOLDER,
            SELF,
        );

        assert!(matches!(superseded, Some(Superseded::ClaimedBy(node)) if node == OTHER));
    }

    #[test]
    fn a_vanished_row_supersedes_our_live_copy() {
        assert!(matches!(
            running_supersession(None, SELF, SELF),
            Some(Superseded::Gone)
        ));
    }

    #[test]
    fn our_own_local_only_row_is_not_superseded() {
        assert!(supersession(
            &entry(PausedRegistryState::LocalOnly, SELF, None),
            &registered_as(SELF)
        )
        .is_none());
    }

    #[test]
    fn a_restart_under_a_new_node_id_does_not_supersede_our_own_records() {
        let ours = entry(PausedRegistryState::Paused, "agentenv-old-pod", None);

        // Same physical node, same artifacts on disk, brand new pod name.
        let after_restart = supersession(&ours, &registered_as("agentenv-old-pod"));

        assert!(
            after_restart.is_none(),
            "a record must be judged against the identity it was registered under"
        );
    }

    #[test]
    fn anonymous_registration_still_yields_to_a_live_holder() {
        assert!(supersession(
            &entry(PausedRegistryState::Running, OTHER, None),
            &ClusterRegistration::Anonymous
        )
        .is_some());
        assert!(supersession(
            &entry(PausedRegistryState::Paused, OTHER, None),
            &ClusterRegistration::Anonymous
        )
        .is_none());
    }

    #[test]
    fn an_answer_naming_this_node_is_not_a_refusal() {
        for claim in [
            ResumeClaim::NotReady {
                origin_node_id: SELF.to_string(),
            },
            ResumeClaim::Conflict {
                origin_node_id: SELF.to_string(),
                reason: crate::orchestrator::ConflictReason::LiveElsewhere,
            },
        ] {
            assert!(
                matches!(
                    arbitration(claim, SELF, ExecutionId::new()),
                    ResumeArbitration::Proceed(_)
                ),
                "a node must not be blocked from resuming by its own hold"
            );
        }
    }

    #[test]
    fn another_node_holding_the_sandbox_blocks_a_local_resume() {
        assert!(matches!(
            arbitration(
                ResumeClaim::Conflict {
                    origin_node_id: OTHER.to_string(),
                    reason: crate::orchestrator::ConflictReason::LiveElsewhere,
                },
                SELF,
                ExecutionId::new(),
            ),
            ResumeArbitration::Blocked { .. }
        ));
        assert!(matches!(
            arbitration(
                ResumeClaim::NotReady {
                    origin_node_id: OTHER.to_string()
                },
                SELF,
                ExecutionId::new(),
            ),
            ResumeArbitration::NotReady { .. }
        ));
    }

    #[test]
    fn an_untracked_sandbox_resumes_without_arbitration() {
        assert!(matches!(
            arbitration(ResumeClaim::NotFound, SELF, ExecutionId::new()),
            ResumeArbitration::Proceed(_)
        ));
    }

    #[test]
    fn a_granted_claim_carries_the_row_for_the_rebuild() {
        let row = entry(PausedRegistryState::Paused, SELF, None);
        let snapshot = row.snapshot_id.clone();

        let claim = ResumeClaim::Claimed {
            entry: Box::new(row),
            previous_state: PausedRegistryState::Paused,
        };
        let ResumeArbitration::Held(held, _) = arbitration(claim, SELF, ExecutionId::new()) else {
            panic!("a granted claim must be held");
        };

        assert_eq!(held.snapshot_id, snapshot);
    }

    #[test]
    fn any_claim_supersedes_a_local_paused_copy() {
        for claimer in [Some(OTHER), None] {
            assert!(
                supersession(
                    &entry(PausedRegistryState::Resuming, SELF, claimer),
                    &registered_as(SELF)
                )
                .is_some(),
                "a resuming row must never leave a local paused copy resumable"
            );
        }
    }

    #[test]
    fn no_registry_row_is_the_only_missing_verdict() {
        assert!(matches!(
            missing_local_verdict(None, None, "self"),
            MissingLocalVerdict::Unknown
        ));
    }

    #[test]
    fn a_resume_in_flight_on_this_node_is_waited_for_not_reported_missing() {
        let row = entry(PausedRegistryState::Resuming, "other", Some("self"));
        assert!(matches!(
            missing_local_verdict(Some(&row), None, "self"),
            MissingLocalVerdict::Wait { .. }
        ));
    }

    #[test]
    fn a_running_local_copy_settles_the_wait() {
        let row = entry(PausedRegistryState::Resuming, "other", Some("self"));
        let local = SandboxMetadata {
            state: SandboxState::Running,
            ..Default::default()
        };
        assert!(matches!(
            missing_local_verdict(Some(&row), Some(&local), "self"),
            MissingLocalVerdict::Ready
        ));
    }

    #[test]
    fn rows_held_elsewhere_are_busy_never_missing() {
        for row in [
            entry(PausedRegistryState::Resuming, "other", Some("other")),
            entry(PausedRegistryState::Running, "other", None),
            entry(PausedRegistryState::Publishing, "other", None),
            entry(PausedRegistryState::LocalOnly, "other", None),
        ] {
            let verdict = missing_local_verdict(Some(&row), None, "self");
            assert!(
                matches!(verdict, MissingLocalVerdict::Busy { .. }),
                "state {:?} must not be reported as missing",
                row.state
            );
        }
    }

    #[test]
    fn a_parked_row_is_rebuildable_from_whichever_process_reads_it() {
        // Production shapes: the origin is a machine, the claimant a process.
        // The two never match, so a verdict that turned on them matching would
        // report the same lie to every replica.
        let row = entry(PausedRegistryState::Paused, HOLDER, None);

        for reader in [SELF, OTHER, HOLDER] {
            assert_eq!(
                missing_local_verdict(Some(&row), None, reader),
                MissingLocalVerdict::Rebuildable,
                "a published parked row is rebuildable, and 'is being resumed by {HOLDER}' is \
                 a statement about nothing that is happening"
            );
        }
    }

    #[test]
    fn a_parked_row_with_nothing_published_stays_busy() {
        let mut row = entry(PausedRegistryState::Paused, HOLDER, None);
        row.snapshot_id = None;

        assert!(
            matches!(
                missing_local_verdict(Some(&row), None, SELF),
                MissingLocalVerdict::Busy { .. }
            ),
            "the control: with no published snapshot there is nothing to rebuild from"
        );
    }
}

#[cfg(test)]
mod cross_node_resume_scope_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::Utc;

    use super::super::paused_coordinator::test_support::CountingRegistry;
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator, PausedSandboxRegistry,
    };
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit, StartedBuild};
    use crate::snapshot::repository::{
        RepositoryError, RepositoryResult, SnapshotListFilter, SnapshotListPage, SnapshotRepository,
    };
    use crate::snapshot::{
        SnapshotAbsence, SnapshotId, SnapshotManager, SnapshotRecord, SnapshotSource,
        TemplateBuildErrorReason,
    };

    /// Catalog fixture exposing one uncommitted row only at `AnyStatus`.
    struct UncommittedSnapshotCatalog {
        row: Option<SnapshotRecord>,
        scoped_reads: Arc<AtomicUsize>,
        /// Optional settled-absence response.
        absence: Option<RepositoryResult<SnapshotAbsence>>,
    }

    #[async_trait]
    impl SnapshotCatalog for UncommittedSnapshotCatalog {
        async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            unreachable!("these tests never create")
        }

        async fn publish_commit(
            &self,
            _commit: SnapshotCommit,
        ) -> RepositoryResult<SnapshotRecord> {
            unreachable!("these tests never commit")
        }

        async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            // Resolvable: an uncommitted row is not one of these.
            Ok(None)
        }

        async fn get_scoped(
            &self,
            _id_or_alias: &str,
            scope: CatalogReadScope,
        ) -> RepositoryResult<Option<SnapshotRecord>> {
            if matches!(scope, CatalogReadScope::AnyStatus) {
                self.scoped_reads.fetch_add(1, Ordering::SeqCst);
                return Ok(self.row.clone());
            }
            Ok(None)
        }

        async fn list_page(
            &self,
            _filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            Ok(SnapshotListPage::single(Vec::new()))
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            Err(RepositoryError::Unsupported {
                feature: "not part of this test".to_string(),
            })
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            Ok(())
        }

        async fn absence_of(&self, _id: &SnapshotId) -> RepositoryResult<SnapshotAbsence> {
            match &self.absence {
                Some(Ok(absence)) => Ok(absence.clone()),
                Some(Err(_)) => Err(RepositoryError::Backend {
                    message: "the catalog mirror cannot be asked".to_string(),
                    source: None,
                }),
                None if self.row.is_some() => {
                    Ok(SnapshotAbsence::unsettled("the catalog holds a row for it"))
                }
                None => Ok(SnapshotAbsence::Settled),
            }
        }
    }

    fn uncommitted(id: &SnapshotId) -> SnapshotRecord {
        SnapshotRecord {
            id: id.clone(),
            alias: None,
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sbx-mid-publish".to_string(),
            },
            resources: Default::default(),
            created_at_unix_ms: 1_700_000_000_000,
            updated_at_unix_ms: 1_700_000_000_000,
            committed: None,
        }
    }

    async fn api_with_catalog(
        row: Option<SnapshotRecord>,
        registry: Arc<CountingRegistry>,
    ) -> (Arc<ApiImpl>, Arc<AtomicUsize>) {
        api_with_absence(row, None, registry).await
    }

    async fn api_with_absence(
        row: Option<SnapshotRecord>,
        absence: Option<RepositoryResult<SnapshotAbsence>>,
        registry: Arc<CountingRegistry>,
    ) -> (Arc<ApiImpl>, Arc<AtomicUsize>) {
        let scoped_reads = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(UncommittedSnapshotCatalog {
            row,
            scoped_reads: Arc::clone(&scoped_reads),
            absence,
        });
        let root = tempfile::tempdir().expect("a temp dir");
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().to_path_buf()),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an orchestrator");
        std::mem::forget(root);

        let snapshot_manager = Arc::new(SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::new(
                catalog,
                Arc::new(crate::snapshot::mock::MockSnapshotArtifactStore),
            )),
            Some(Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver)),
            None,
        ));

        let api = Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry as Arc<dyn PausedSandboxRegistry>,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::node_local(NodeIdentity::from_config(&Default::default()).id),
        ));
        (api, scoped_reads)
    }

    fn claimed_entry(snapshot_id: SnapshotId) -> PausedSandboxEntry {
        let sandbox_id = SandboxId::new();
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Resuming,
            generation: 1,
            origin_node_id: "node-b".to_string(),
            claimed_by_node_id: Some("node-a".to_string()),
            snapshot_id: Some(snapshot_id),
            metadata: Some(SandboxMetadata {
                id: sandbox_id,
                ..Default::default()
            }),
            execution_id: Some(ExecutionId::new()),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_snapshot_that_has_not_finished_publishing_does_not_drop_the_registry_row() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::new(0, false));
        let (api, scoped_reads) =
            api_with_catalog(Some(uncommitted(&snapshot_id)), Arc::clone(&registry)).await;

        let outcome = api
            .restore_claimed_sandbox(claimed_entry(snapshot_id), NewTimeout::None)
            .await;

        assert!(
            matches!(outcome, CrossNodeResume::Failed(_)),
            "an unfinished publish is a retryable failure, not a sandbox that does not exist"
        );
        assert_eq!(
            registry.remove_calls(),
            0,
            "🔴 the assertion. Dropping this row throws away the cluster's only record of a \
             paused sandbox whose bytes are sitting in object storage, intact"
        );
        assert_eq!(
            scoped_reads.load(Ordering::SeqCst),
            1,
            "and the question has to be asked at the scope that can tell the two states apart"
        );
    }

    #[tokio::test]
    async fn a_snapshot_whose_publish_is_still_queued_does_not_drop_the_registry_row() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::new(0, false));
        let (api, _) = api_with_absence(
            None,
            Some(Ok(SnapshotAbsence::unsettled(
                "the central catalog is owed a 'publish_commit' for it",
            ))),
            Arc::clone(&registry),
        )
        .await;

        let outcome = api
            .restore_claimed_sandbox(claimed_entry(snapshot_id), NewTimeout::None)
            .await;

        assert!(
            matches!(outcome, CrossNodeResume::Failed(_)),
            "a publish still on its way is a retryable failure, not a sandbox that does not exist"
        );
        assert_eq!(
            registry.remove_calls(),
            0,
            "🔴 the assertion. The read side holds nothing and it is still not proof: the write \
             is queued, the bytes are durable, and dropping this row loses the sandbox for good"
        );
    }

    #[tokio::test]
    async fn a_catalog_that_cannot_settle_an_absence_does_not_drop_the_registry_row() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::new(0, false));
        let (api, _) = api_with_absence(
            None,
            Some(Err(RepositoryError::Backend {
                message: "unreachable".to_string(),
                source: None,
            })),
            Arc::clone(&registry),
        )
        .await;

        let outcome = api
            .restore_claimed_sandbox(claimed_entry(snapshot_id), NewTimeout::None)
            .await;

        assert!(
            matches!(outcome, CrossNodeResume::Failed(_)),
            "a store nobody could reach has not said the snapshot is gone"
        );
        assert_eq!(
            registry.remove_calls(),
            0,
            "and the row must survive a failure to look at least as well as it survives a look"
        );
    }

    #[tokio::test]
    async fn a_snapshot_no_row_exists_for_still_drops_the_registry_row() {
        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::new(0, false));
        let (api, _) = api_with_catalog(None, Arc::clone(&registry)).await;

        let outcome = api
            .restore_claimed_sandbox(claimed_entry(snapshot_id), NewTimeout::None)
            .await;

        assert!(
            matches!(outcome, CrossNodeResume::NotFound),
            "a snapshot nothing holds is a sandbox that cannot be rebuilt"
        );
        assert_eq!(
            registry.remove_calls(),
            1,
            "and the dangling row goes with it"
        );
    }
}

#[cfg(test)]
mod cross_node_resume_source_tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::super::paused_coordinator::test_support::CountingRegistry;
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator, PausedSandboxRegistry,
    };
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::unresolvable_snapshot_manager;
    use crate::snapshot::{CommittedSnapshot, SnapshotId, SnapshotManager, SnapshotRecord};

    /// API fixture whose ready snapshot cannot be resolved into local runtime files.
    async fn api_with(row: SnapshotRecord) -> Arc<ApiImpl> {
        let root = tempfile::tempdir().expect("a temp dir");
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().to_path_buf()),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an orchestrator");
        std::mem::forget(root);

        let snapshot_manager: Arc<SnapshotManager> = Arc::new(unresolvable_snapshot_manager(row));
        let registry = Arc::new(CountingRegistry::new(0, false)) as Arc<dyn PausedSandboxRegistry>;

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry,
                Arc::clone(&snapshot_manager),
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    fn claimed_entry(snapshot_id: SnapshotId) -> PausedSandboxEntry {
        let sandbox_id = SandboxId::new();
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Resuming,
            generation: 1,
            origin_node_id: "node-b".to_string(),
            claimed_by_node_id: Some("node-a".to_string()),
            snapshot_id: Some(snapshot_id),
            metadata: Some(SandboxMetadata {
                id: sandbox_id,
                ..Default::default()
            }),
            execution_id: Some(ExecutionId::new()),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_cross_node_resume_ships_the_catalog_row_without_resolving_it() {
        let row = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let snapshot_id = row.id.clone();

        let api = api_with(row).await;
        let outcome = api
            .restore_claimed_sandbox(claimed_entry(snapshot_id), NewTimeout::None)
            .await;
        assert!(
            matches!(outcome, CrossNodeResume::Restored(_)),
            "🔴 the assertion. This manager's runtime resolver fails every call, so a restore \
             that touched it could not have got here — restoring over it is the proof that this \
             path never turned the catalog row into local bytes, got {outcome:?}"
        );
    }
}

#[cfg(test)]
mod absent_capture_tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use agentenv_http_server::apis::sandboxes::{Sandboxes, SandboxesSandboxIdResumePostResponse};
    use agentenv_http_server::models;
    use chrono::Utc;

    use super::super::paused_coordinator::test_support::CountingRegistry;
    use super::*;
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator, PausedSandboxRegistry,
        SandboxPersister,
    };
    use crate::sandbox::mock::{MockBackendFactory, MockBehavior, MockSnapshot};
    use crate::snapshot::mock::unresolvable_snapshot_manager;
    use crate::snapshot::{CommittedSnapshot, SnapshotId, SnapshotManager, SnapshotRecord};

    /// The machine that took the capture. Never a claimant identity.
    const ORIGIN: &str = "aenv-node-203";

    /// Where the holding node keeps the record a reopen needs.
    fn record_path(root: &Path, sandbox_id: SandboxId) -> PathBuf {
        root.join("records").join(format!("{sandbox_id}.json"))
    }

    fn parked_row(sandbox_id: SandboxId, snapshot_id: Option<SnapshotId>) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state: match snapshot_id {
                Some(_) => PausedRegistryState::Paused,
                None => PausedRegistryState::LocalOnly,
            },
            generation: 3,
            origin_node_id: ORIGIN.to_string(),
            claimed_by_node_id: None,
            snapshot_id,
            metadata: Some(SandboxMetadata {
                id: sandbox_id,
                ..Default::default()
            }),
            execution_id: None,
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Writes the capture record and artifacts a node holds for a paused sandbox.
    ///
    /// Call before building the orchestrator: startup is what loads it.
    async fn seed_capture(root: &Path, sandbox_id: SandboxId) {
        let persister = FileBackedSandboxPersister::new_for_test(root.to_path_buf());
        let artifacts = root.join("artifacts").join(sandbox_id.to_string());
        std::fs::create_dir_all(&artifacts).expect("an artifact directory");

        persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    ..Default::default()
                },
                Some(&artifacts),
                &MockSnapshot,
            )
            .await
            .expect("a persisted capture");
        persister
            .mark_cluster_registered(&sandbox_id, ORIGIN)
            .await
            .expect("a registered capture");
    }

    async fn api_over_capture(
        root: &Path,
        sandbox_id: SandboxId,
        registry: Arc<CountingRegistry>,
    ) -> Arc<ApiImpl> {
        let behavior = Arc::new(MockBehavior::new());
        behavior.reopen_needs_capture_record(record_path(root, sandbox_id), ORIGIN, sandbox_id);

        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::with_behavior(behavior),
            FileBackedSandboxPersister::new_for_test(root.to_path_buf()),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an orchestrator");

        let snapshot_manager: Arc<SnapshotManager> = Arc::new(unresolvable_snapshot_manager(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
        ));

        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                registry as Arc<dyn PausedSandboxRegistry>,
                snapshot_manager,
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    async fn resume(api: &ApiImpl, sandbox_id: SandboxId) -> SandboxesSandboxIdResumePostResponse {
        api.sandboxes_sandbox_id_resume_post(
            &http::Method::POST,
            &headers::Host::from(http::uri::Authority::from_static("localhost")),
            &axum_extra::extract::CookieJar::new(),
            &super::super::Claims,
            &models::SandboxesSandboxIdResumePostPathParams {
                sandbox_id: sandbox_id.to_string(),
            },
            &models::ResumedSandbox::new(),
        )
        .await
        .expect("the handler answers rather than failing the request")
    }

    #[tokio::test]
    async fn a_published_sandbox_whose_node_lost_its_capture_resumes_by_rebuilding() {
        let root = tempfile::tempdir().expect("a temp dir");
        let sandbox_id = SandboxId::new();
        seed_capture(root.path(), sandbox_id).await;

        let snapshot_id = SnapshotId::generate();
        let registry = Arc::new(CountingRegistry::holding(parked_row(
            sandbox_id,
            Some(snapshot_id),
        )));
        let api = api_over_capture(root.path(), sandbox_id, Arc::clone(&registry)).await;

        // The pve-mf failure, reproduced: the row and the repository bytes are
        // intact, and only the holding node's capture record is gone.
        std::fs::remove_file(record_path(root.path(), sandbox_id))
            .expect("the capture record was written by the seed");

        let answer = resume(&api, sandbox_id).await;

        assert!(
            matches!(
                answer,
                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
            ),
            "🔴 the assertion. A published row plus repository bytes is everything a rebuild \
             needs; answering anything else here is the deterministic 500 this branch exists \
             to remove, got {answer:?}"
        );
        assert_eq!(
            registry.claimed_as(),
            vec![api.paused.node_id().to_string()],
            "the rebuild has to run under the claim this process took, not under the origin"
        );

        let granted = registry
            .granted_execution()
            .expect("the claim allocated an incarnation");
        let marked = registry.marked_running();
        let [(holder, ran_as)] = marked.as_slice() else {
            panic!("the rebuild has to record the sandbox as running exactly once: {marked:?}");
        };
        assert_eq!(
            *ran_as, granted,
            "🔴 the rebuild has to finish under the incarnation its claim allocated. Minting a \
             fresh one leaves mark_running matching no row, which strands the claim in \
             'resuming' forever"
        );
        assert_ne!(
            holder.as_str(),
            ORIGIN,
            "🔴 and the holder it records has to be the machine that actually rebuilt it. \
             mark_running is the only writer of origin_node_id, so this call is the origin \
             rewrite: without it the row keeps pointing at a machine holding nothing"
        );
    }

    #[tokio::test]
    async fn an_unpublished_sandbox_whose_node_lost_its_capture_is_still_refused() {
        let root = tempfile::tempdir().expect("a temp dir");
        let sandbox_id = SandboxId::new();
        seed_capture(root.path(), sandbox_id).await;

        let registry = Arc::new(CountingRegistry::holding(parked_row(sandbox_id, None)));
        let api = api_over_capture(root.path(), sandbox_id, Arc::clone(&registry)).await;

        std::fs::remove_file(record_path(root.path(), sandbox_id))
            .expect("the capture record was written by the seed");

        let answer = resume(&api, sandbox_id).await;

        let SandboxesSandboxIdResumePostResponse::Status409_Conflict(error) = answer else {
            panic!(
                "the control. Nothing outside the origin has these bytes, so this resume must \
                 still be refused, got {answer:?}"
            );
        };
        assert!(
            error.message.contains(ORIGIN),
            "the refusal has to name the machine that holds the only copy: {error:?}"
        );
        assert!(
            api.orchestrator()
                .get_sandbox(&sandbox_id)
                .await
                .expect("the store answers")
                .is_some(),
            "a refused resume must leave the local record alone; discarding it here would \
             throw away the only pointer to the only copy"
        );
    }
}
