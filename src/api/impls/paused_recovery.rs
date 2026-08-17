//! Cross-node pause and resume.
//!
//! A pause leaves the sandbox resumable on its own node through the node-local
//! persister. That is the fast path and it is untouched here. What this module
//! adds is the slow path: publishing the very same capture to the shared
//! snapshot repository and recording it in the cluster registry, so a resume
//! that arrives at *any* node can rebuild the sandbox under its original ID —
//! including when the node that paused it is gone for good.
//!
//! Everything here is inert unless the paused-sandbox registry is configured
//! with a cluster backend.

use std::time::SystemTime;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use super::ApiImpl;
use crate::orchestrator::{
    CreateSandboxRequest, NewTimeout, PauseOutcome, PausedRegistryState, PausedSandboxEntry,
    ResumeClaim, SandboxLaunchSource, SandboxListFilter, SandboxMetadata,
};
use crate::snapshot::{SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource};
use crate::types::SandboxId;

/// Outcome of trying to resume a sandbox this node has never seen.
pub(super) enum CrossNodeResume {
    /// The sandbox is running here again, under its original ID.
    Restored(Box<SandboxMetadata>),
    /// The cluster has no paused record for it.
    NotFound,
    /// The snapshot has not landed in the repository yet, so only the origin
    /// node can serve this resume.
    NotReady { origin_node_id: String },
    /// Another node is already resuming it.
    Conflict { origin_node_id: String },
    /// The record exists and was claimed, but rebuilding failed.
    Failed(String),
}

impl ApiImpl {
    /// Makes a just-paused sandbox recoverable beyond this node.
    ///
    /// Deliberately best-effort: by the time this runs the sandbox is already
    /// paused, persisted and locally resumable, so every failure below costs
    /// cross-node recovery and nothing else. Turning a published-snapshot
    /// failure into a pause failure would trade a working pause for a broken
    /// one.
    pub(super) async fn register_paused_sandbox(
        &self,
        sandbox_id: SandboxId,
        outcome: PauseOutcome,
    ) {
        // No publishable capture means the pause wrote into backend-managed
        // temporaries that no other node could ever read.
        let Some(publishable) = outcome.publishable else {
            return;
        };

        let entry = PausedSandboxEntry {
            sandbox_id,
            // Filled in by the registry from its own configured cluster.
            cluster_id: uuid::Uuid::nil(),
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: self.node_id.clone(),
            snapshot_id: None,
            metadata: outcome.metadata.clone(),
            paused_at: system_time_to_utc(outcome.metadata.created_at),
            updated_at: Utc::now(),
        };

        let generation = match self.paused_registry.begin_pause(&entry).await {
            Ok(generation) => {
                // From here on the cluster knows about this sandbox, so a later
                // reconciliation is allowed to act on its absence from the
                // registry. Recording that on the local record is what keeps
                // reconciliation off records that predate the registry.
                if let Err(err) = self
                    .orchestrator
                    .mark_paused_record_cluster_registered(sandbox_id)
                    .await
                {
                    warn!(error = ?err, %sandbox_id, "failed to mark the local record as registered");
                }

                generation
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to register paused sandbox; it stays resumable on this node only"
                );
                return;
            }
        };

        let published = self
            .snapshot_manager
            .publish_captured(
                self.publish_metadata_for_pause(&outcome.metadata),
                publishable,
            )
            .await;

        match published {
            Ok(record) => {
                let snapshot_id = record.id.clone();
                if let Err(err) = self
                    .paused_registry
                    .complete_pause(&sandbox_id, generation, &snapshot_id)
                    .await
                {
                    // The snapshot is durable but the registry disagrees, which
                    // means some other writer moved the sandbox on while we
                    // published. Leave their decision alone.
                    warn!(
                        error = %err,
                        %sandbox_id,
                        %snapshot_id,
                        "published paused snapshot but could not mark it durable"
                    );

                    return;
                }

                info!(%sandbox_id, %snapshot_id, "paused sandbox is recoverable cluster-wide");
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %sandbox_id,
                    "failed to publish paused snapshot; it stays resumable on this node only"
                );
                // Keep the row and downgrade it instead of deleting it: the
                // sandbox is genuinely paused here, just not recoverable
                // anywhere else. A deleted row would be indistinguishable from
                // "already resumed elsewhere", and reconciliation would then
                // throw away the only copy that exists.
                if let Err(err) = self
                    .paused_registry
                    .mark_local_only(&sandbox_id, generation)
                    .await
                {
                    warn!(error = %err, %sandbox_id, "failed to mark the registry row local-only");
                }
            }
        }
    }

    /// Rebuilds a sandbox this node has never run, from the cluster registry.
    ///
    /// Called only after the local resume reported the sandbox unknown, so a
    /// node that holds the sandbox locally never reaches this path.
    pub(super) async fn resume_from_registry(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> CrossNodeResume {
        let claim = match self
            .paused_registry
            .claim_for_resume(&sandbox_id, &self.node_id)
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to claim paused sandbox for resume");

                return CrossNodeResume::Failed(err.to_string());
            }
        };

        let entry = match claim {
            ResumeClaim::Claimed(entry) => *entry,
            ResumeClaim::NotFound => return CrossNodeResume::NotFound,
            ResumeClaim::NotReady { origin_node_id } => {
                return CrossNodeResume::NotReady { origin_node_id }
            }
            ResumeClaim::Conflict { origin_node_id } => {
                return CrossNodeResume::Conflict { origin_node_id }
            }
        };

        // Decoding guarantees a paused entry names its snapshot, so this cannot
        // be None here; treat it as a failed claim rather than panicking.
        let Some(snapshot_id) = entry.snapshot_id.clone() else {
            self.release_claim(&sandbox_id, entry.generation).await;

            return CrossNodeResume::Failed("paused sandbox has no published snapshot".to_string());
        };

        info!(
            %sandbox_id,
            %snapshot_id,
            origin_node_id = %entry.origin_node_id,
            "restoring paused sandbox from another node's snapshot"
        );

        let snapshot = match self
            .snapshot_manager
            .load_runnable(&snapshot_id.to_string())
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                // The registry names a snapshot the repository no longer has.
                // Releasing the claim would just make the next resume fail the
                // same way, so drop the record and report it as unknown.
                warn!(%sandbox_id, %snapshot_id, "paused snapshot is missing from the repository");
                if let Err(err) = self.paused_registry.remove(&sandbox_id).await {
                    warn!(error = %err, %sandbox_id, "failed to drop the dangling registry row");
                }

                return CrossNodeResume::NotFound;
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, %snapshot_id, "failed to load paused snapshot");
                self.release_claim(&sandbox_id, entry.generation).await;

                return CrossNodeResume::Failed(format!("failed to load paused snapshot: {err}"));
            }
        };

        let request = restore_request(&entry.metadata, snapshot, timeout);

        match self.orchestrator.restore_sandbox(sandbox_id, request).await {
            Ok(metadata) => {
                // The sandbox is running here now, so the paused record has
                // served its purpose. A stale row left behind would let a
                // later resume try to start a second copy from the snapshot.
                self.forget_paused_sandbox(sandbox_id).await;

                CrossNodeResume::Restored(Box::new(metadata))
            }
            Err(err) => {
                warn!(error = ?err, %sandbox_id, "failed to restore paused sandbox");
                self.release_claim(&sandbox_id, entry.generation).await;

                CrossNodeResume::Failed(err.to_string())
            }
        }
    }

    /// Drops a sandbox's paused record and the snapshot that backed it.
    ///
    /// Called once the sandbox is running again, on either resume path. The
    /// snapshot exists purely to carry a paused sandbox between nodes, so
    /// leaving it behind would both leak repository layers and surface an
    /// internal artifact in the user-visible snapshot listing.
    pub(super) async fn forget_paused_sandbox(&self, sandbox_id: SandboxId) {
        let entry = match self.paused_registry.get(&sandbox_id).await {
            Ok(Some(entry)) => entry,
            // Nothing recorded (the common case with a node-local registry).
            Ok(None) => return,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "failed to read the paused registry row");

                return;
            }
        };

        if let Err(err) = self.paused_registry.remove(&sandbox_id).await {
            warn!(error = %err, %sandbox_id, "failed to clear the paused registry row");

            // Keep the snapshot: the row still points at it.
            return;
        }

        if let Some(snapshot_id) = entry.snapshot_id {
            if let Err(err) = self.snapshot_manager.delete(snapshot_id.to_string()).await {
                warn!(error = ?err, %sandbox_id, %snapshot_id, "failed to delete the paused snapshot");
            }
        }
    }

    fn publish_metadata_for_pause(&self, metadata: &SandboxMetadata) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            // Unnamed on purpose: this snapshot is an implementation detail of
            // pause, not something a user asked to be able to launch by name.
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: metadata.id.to_string(),
            },
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            resources: metadata.resources,
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            custom_extension_params: metadata.custom_extension_params.clone(),
        }
    }

    /// Drops local paused records for sandboxes the cluster no longer knows about.
    ///
    /// Runs once at startup. A node that comes back after being away can hold
    /// paused records for sandboxes another node has since resumed — the record
    /// and its artifacts survive on local disk because they live on a host path.
    /// Left alone, a resume aimed at this node would start a second copy of a
    /// sandbox that is already running elsewhere, and the two would write to
    /// their own rootfs layers from the same starting point.
    ///
    /// Absence in the registry is the signal, which is exactly why a pause that
    /// failed to publish keeps a `local_only` row rather than deleting it: here,
    /// a missing row must only ever mean "gone", never "never registered".
    pub async fn reconcile_local_paused_records(&self) {
        // A disabled registry reports every sandbox as missing, which this
        // would read as "all of them moved on".
        if !self.paused_registry.is_cluster_backed() {
            return;
        }

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

        let mut discarded = 0usize;
        for metadata in paused {
            let sandbox_id = metadata.id;

            // Never reason about a record the cluster was never told about:
            // for those the local copy is the only copy. This is also what
            // makes switching an existing node from the node-local backend to
            // a cluster one safe — every record it already holds is untouched.
            match self
                .orchestrator
                .paused_record_is_cluster_registered(sandbox_id)
                .await
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    warn!(error = ?err, %sandbox_id, "failed to read local registration marker");
                    continue;
                }
            }

            match self.paused_registry.get(&sandbox_id).await {
                // Still known to the cluster in some form; leave it alone.
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(err) => {
                    // Never guess when the registry cannot answer: one unreadable
                    // response must not cascade into deleting local records.
                    warn!(
                        error = %err,
                        %sandbox_id,
                        "registry unreadable; stopping paused-record reconciliation"
                    );

                    return;
                }
            }

            match self
                .orchestrator
                .discard_local_paused_record(sandbox_id)
                .await
            {
                Ok(true) => discarded += 1,
                Ok(false) => {}
                Err(err) => {
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

    /// Discards this node's paused record when the cluster says the sandbox has
    /// moved on, so a resume cannot start a second copy.
    ///
    /// Returns whether a record was discarded. Any doubt — registry disabled,
    /// registry unreachable, row still present — leaves the local record alone:
    /// refusing a resume that would have worked is worse than the narrow race
    /// this closes.
    pub(super) async fn discard_if_superseded(&self, sandbox_id: SandboxId) -> bool {
        if !self.paused_registry.is_cluster_backed() {
            return false;
        }

        // Same rule as reconciliation: absence only means something for a
        // record the cluster was told about.
        if !matches!(
            self.orchestrator
                .paused_record_is_cluster_registered(sandbox_id)
                .await,
            Ok(true)
        ) {
            return false;
        }

        match self.paused_registry.get(&sandbox_id).await {
            Ok(None) => {}
            Ok(Some(_)) => return false,
            Err(err) => {
                warn!(error = %err, %sandbox_id, "registry unreadable; serving resume locally");

                return false;
            }
        }

        matches!(
            self.orchestrator
                .discard_local_paused_record(sandbox_id)
                .await,
            Ok(true)
        )
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) {
        if let Err(err) = self
            .paused_registry
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

/// Builds the launch request that brings a paused sandbox back.
///
/// Every field is carried over from the record the pausing node wrote, so the
/// restored sandbox keeps the timeout policy, metadata, network policy and
/// extension params it had. `env_vars` is intentionally absent: environment is
/// applied at first boot and is already baked into the snapshot.
fn restore_request(
    metadata: &SandboxMetadata,
    snapshot: crate::snapshot::RunnableSnapshot,
    timeout: NewTimeout,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(snapshot)),
        // A restore is a resume: the request's timeout wins when it set one,
        // otherwise the sandbox keeps the timeout it was paused with.
        timeout: match timeout {
            NewTimeout::Set(duration) | NewTimeout::EnsureMinimum(duration) => Some(duration),
            NewTimeout::UseExisting => metadata.timeout,
            NewTimeout::None => None,
        },
        timeout_action: metadata.timeout_action,
        auto_resume: metadata.auto_resume,
        user_metadata: metadata.user_metadata.clone(),
        env_vars: None,
        network_policy: metadata.network_policy.clone(),
        secure: metadata.secure,
        custom_extension_params: metadata.custom_extension_params.clone(),
    }
}

fn system_time_to_utc(value: SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(value)
}
