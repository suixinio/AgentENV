//! The cluster registry, reached through whoever owns the database.
//!
//! Same registry and same semantics as [`PostgresPausedSandboxRegistry`](super::PostgresPausedSandboxRegistry);
//! the difference is who holds the connection. Here the node states what it
//! wants over gRPC and the controller runs the statement, so the DSN, the
//! connection budget and the schema stop being the business of every machine
//! that runs user code.
//!
//! 🔴 **A failure is never an answer.** Everything below turns any transport
//! failure into [`PausedRegistryError::Backend`] and never into an empty
//! result, because an empty result is a real answer with real consequences: a
//! sandbox missing from a batch read means its row is gone, and reconciliation
//! deletes local artifacts and tears down running sandboxes on the strength of
//! that. The same rule covers a response that arrived but says nothing — an
//! `AcquireSandbox` with no outcome set is a protocol failure, not a
//! `NotFound`, and reading it as one would hand out a resume the cluster never
//! authorised.
//!
//! What travels over the wire and what does not is set out in
//! `services/api/proto/scheduler.proto`: five methods for the thirteen here,
//! and the sandbox record only on the two calls that need it.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    log_claim_outcome, BeganPause, ConflictReason, HeldSandbox, MarkRunningOutcome,
    PausedRegistryError, PausedRegistryState, PausedSandboxEntry, PausedSandboxRegistry,
    ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
};
use crate::orchestrator::store::SandboxMetadata;
use crate::proto::scheduler as pb;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

/// How long any one registry call may take.
///
/// Matches the heartbeat client's budget: the calls here sit on the resume and
/// reconciliation paths, and a request that has not been answered in ten
/// seconds has already cost more than failing it would.
const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest number of sandbox IDs asked for in a single `GetSandboxes` call.
///
/// The same bound the direct backend puts on its parameter arrays, kept for the
/// same reason in a different currency: it keeps one node's whole roster from
/// building a request — or a response — large enough to be refused, and every
/// chunk still has to succeed for the call to succeed.
const GET_MANY_CHUNK: usize = 1_000;

/// gRPC-backed [`PausedSandboxRegistry`].
pub struct CentralPausedSandboxRegistry {
    /// The connection pool. Cloning it is cheap and shares the same
    /// connection, so each call makes its own client over this.
    channel: Channel,
    cluster_id: Uuid,
    /// This machine's identity, used by the transitions the trait does not hand
    /// one — the conditional writes, which are arbitrated by generation rather
    /// than by who is asking.
    node_id: String,
    /// 🔴 The lease length travels with every write rather than being the
    /// controller's to choose. The node is what renews, on a cadence its own
    /// configuration ties the TTL to (`lease_ttl_secs()` is held to three
    /// renewal intervals), so a TTL decided anywhere else would expire rows
    /// under a node that is renewing exactly as it was told to.
    lease_ttl_millis: i64,
    /// How often this node renews, reported alongside every renewal.
    ///
    /// The invariant that owns both numbers — a lease must outlive two missed
    /// renewals — is enforced locally by `lease_ttl_secs()`, which cannot see a
    /// node built against a different default or a config whose two knobs have
    /// drifted apart. The controller can see both and says so when they
    /// disagree; it does not refuse, because refusing a renewal is how a
    /// configuration smell becomes the outage the invariant exists to prevent.
    reconcile_interval_millis: i64,
    call_timeout: Duration,
}

impl CentralPausedSandboxRegistry {
    /// Builds a registry pointed at `endpoint`.
    ///
    /// Lazy, like every other gRPC client here: the endpoint is parsed now and
    /// dialled on first use, so a controller that is still rolling does not
    /// stop the node from starting. What that costs — a startup-time release
    /// that fails and used to never be retried — is paid for separately, by
    /// making that release retryable while it is still exact.
    pub fn connect_lazy(
        endpoint: &str,
        cluster_id: Uuid,
        node_id: String,
        lease_ttl_secs: u64,
        reconcile_interval_secs: u64,
    ) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| anyhow!("invalid scheduler endpoint '{endpoint}': {e}"))?
            .connect_lazy();

        Ok(Self::over_channel(
            channel,
            cluster_id,
            node_id,
            lease_ttl_secs,
            reconcile_interval_secs,
        ))
    }

    fn over_channel(
        channel: Channel,
        cluster_id: Uuid,
        node_id: String,
        lease_ttl_secs: u64,
        reconcile_interval_secs: u64,
    ) -> Self {
        Self {
            channel,
            cluster_id,
            node_id,
            lease_ttl_millis: i64::try_from(lease_ttl_secs.saturating_mul(1_000))
                .unwrap_or(i64::MAX),
            reconcile_interval_millis: i64::try_from(reconcile_interval_secs.saturating_mul(1_000))
                .unwrap_or(i64::MAX),
            call_timeout: GRPC_CALL_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_call_timeout(mut self, call_timeout: Duration) -> Self {
        self.call_timeout = call_timeout;

        self
    }

    fn client(&self) -> pb::paused_registry_client::PausedRegistryClient<Channel> {
        pb::paused_registry_client::PausedRegistryClient::new(self.channel.clone())
    }

    /// Wraps a message in a request carrying the call budget.
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(self.call_timeout);

        request
    }

    /// The one place a transport outcome becomes a registry error.
    ///
    /// Every status code lands here, including the ones that sound like
    /// answers: `NOT_FOUND` is the controller failing to serve the call, not
    /// the sandbox being absent — absence is an empty list in a successful
    /// response. Deadlines, refused connections, half-read streams and bodies
    /// that will not decode all arrive as a status too, and all of them mean
    /// the same thing to a caller: nothing was learned.
    fn unreachable(operation: &'static str, status: tonic::Status) -> PausedRegistryError {
        PausedRegistryError::backend(
            operation,
            anyhow!("{}: {}", status.code(), status.message().to_owned()),
        )
    }

    /// A well-formed response that does not say what it must.
    ///
    /// Kept apart from the row-level decode failures below: those describe one
    /// unreadable sandbox, this describes a controller that answered off
    /// contract, and reading either as "no such sandbox" is the mistake this
    /// whole module is arranged around.
    fn malformed(operation: &'static str, reason: &'static str) -> PausedRegistryError {
        PausedRegistryError::backend(
            operation,
            anyhow!("registry answered off contract: {reason}"),
        )
    }

    fn invalid(sandbox_id: &str, reason: String) -> PausedRegistryError {
        PausedRegistryError::InvalidRecord {
            sandbox_id: sandbox_id.to_string(),
            reason,
            source: None,
        }
    }

    /// Turns one row on the wire into an entry, refusing anything it cannot
    /// read rather than passing on a plausible-looking record.
    ///
    /// The checks are the direct backend's read-time invariants, moved to the
    /// side of the wire that acts on them. They matter more here, not less:
    /// there they guarded against a corrupted row, here they also guard against
    /// a controller that has drifted from this build — including one answering
    /// for a different cluster, which the direct backend rules out in the
    /// `WHERE` clause of every statement and which nothing else rules out once
    /// the scope is something a request merely asks for.
    fn decode_entry(
        &self,
        row: pb::RegistryEntry,
        metadata: Option<SandboxMetadata>,
    ) -> RegistryResult<PausedSandboxEntry> {
        let sandbox_id = SandboxId::parse_str(&row.sandbox_id)
            .map_err(|_| Self::invalid(&row.sandbox_id, "sandbox id is not a uuid".to_string()))?;

        let cluster_id = Uuid::parse_str(&row.cluster_id)
            .map_err(|_| Self::invalid(&row.sandbox_id, "cluster id is not a uuid".to_string()))?;
        if cluster_id != self.cluster_id {
            return Err(Self::invalid(
                &row.sandbox_id,
                format!(
                    "registry answered with cluster '{cluster_id}' for a node in cluster '{}'",
                    self.cluster_id
                ),
            ));
        }

        let state = PausedRegistryState::parse(&row.state).ok_or_else(|| {
            Self::invalid(&row.sandbox_id, format!("unknown state '{}'", row.state))
        })?;

        let snapshot_id = if row.snapshot_id.is_empty() {
            None
        } else {
            Some(SnapshotId::from_uuid(
                Uuid::parse_str(&row.snapshot_id).map_err(|_| {
                    Self::invalid(&row.sandbox_id, "snapshot id is not a uuid".to_string())
                })?,
            ))
        };

        // A durable row must name the snapshot it can be rebuilt from; without
        // it the entry promises a cross-node resume it cannot deliver.
        if state == PausedRegistryState::Paused && snapshot_id.is_none() {
            return Err(Self::invalid(
                &row.sandbox_id,
                "paused entry carries no snapshot reference".to_string(),
            ));
        }

        Ok(PausedSandboxEntry {
            sandbox_id,
            cluster_id,
            state,
            generation: row.generation,
            origin_node_id: row.origin_node_id,
            claimed_by_node_id: Some(row.claimed_by_node_id).filter(|node| !node.is_empty()),
            snapshot_id,
            metadata,
            paused_at: decode_micros(&row.sandbox_id, row.paused_at_unix_micros, "paused_at")?,
            updated_at: decode_micros(&row.sandbox_id, row.updated_at_unix_micros, "updated_at")?,
        })
    }

    /// Reads rows in bulk, all or nothing.
    ///
    /// A chunk that fails fails the whole call, and a row that will not decode
    /// fails it too: a shorter map is indistinguishable from rows that do not
    /// exist, which is a deletion instruction.
    async fn fetch(
        &self,
        sandbox_ids: &[SandboxId],
        operation: &'static str,
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
        // No explicit empty case: `chunks` yields nothing for an empty slice, so
        // an idle node makes no call at all without anything here saying so.
        let mut rows = HashMap::with_capacity(sandbox_ids.len());
        for chunk in sandbox_ids.chunks(GET_MANY_CHUNK) {
            let response = self
                .client()
                .get_sandboxes(self.request(pb::GetSandboxesRequest {
                    cluster_id: self.cluster_id.to_string(),
                    sandbox_ids: chunk.iter().map(SandboxId::to_string).collect(),
                }))
                .await
                .map_err(|status| Self::unreachable(operation, status))?
                .into_inner();

            // 🔴 Check the answer covers what was asked before reading it.
            //
            // Everything downstream treats an id's absence from `rows` as "the
            // cluster has no row for it", and answers that by tearing down the
            // running VM and deleting its local artifacts. All-or-nothing is
            // the contract, but it is a property of the *controller's*
            // implementation, and from here a response that lost rows on the
            // way — a truncated page, a dropped chunk, a proxy that shortened
            // the body — is indistinguishable from one that found none.
            //
            // So the coverage is asserted rather than assumed. Failing here
            // costs one reconciliation pass, which the caller already knows how
            // to skip; not failing here costs a workspace.
            Self::require_full_coverage(operation, chunk, &response.covered_sandbox_ids)?;

            for row in response.sandboxes {
                let entry = self.decode_entry(row, None)?;
                rows.insert(entry.sandbox_id, entry);
            }
        }

        Ok(rows)
    }

    /// Fails unless the controller says it looked up every id in `chunk`.
    ///
    /// An older controller reports nothing, and that is the one case this lets
    /// through: it is a rollout, not a truncation, and the same node was
    /// running against that controller without any coverage check at all a
    /// moment ago. Refusing it would turn every mixed-version window into a
    /// node-wide reconciliation stall.
    fn require_full_coverage(
        operation: &'static str,
        requested: &[SandboxId],
        covered: &[String],
    ) -> RegistryResult<()> {
        if covered.is_empty() {
            return Ok(());
        }

        let covered: HashSet<&str> = covered.iter().map(String::as_str).collect();
        let missing = requested
            .iter()
            .map(SandboxId::to_string)
            .find(|id| !covered.contains(id.as_str()));

        match missing {
            None => Ok(()),
            Some(id) => {
                warn!(
                    sandbox_id = %id,
                    requested = requested.len(),
                    covered = covered.len(),
                    "the registry answered a batch it did not claim to have looked up in full"
                );

                Err(Self::malformed(
                    operation,
                    "a batch read that did not cover every id it was asked for",
                ))
            }
        }
    }

    /// Sends one state transition and hands back what the controller said.
    #[allow(clippy::too_many_arguments)]
    async fn transition(
        &self,
        operation: &'static str,
        kind: pb::TransitionKind,
        sandbox_id: &SandboxId,
        node_id: &str,
        expect_generation: Option<i64>,
        metadata_json: Vec<u8>,
        snapshot_id: String,
    ) -> RegistryResult<pb::TransitionSandboxResponse> {
        // 🔴 The lease travels with every transition that stamps one, which is
        // all of them but `remove`: a row that is being deleted has no lease
        // left to stamp, and sending a TTL to a statement that does not write
        // one would read as though this call renewed something. Zero is how the
        // wire says "not carried" for a plain integer field.
        let lease_ttl_millis = match kind {
            pb::TransitionKind::Remove => 0,
            _ => self.lease_ttl_millis,
        };

        let response = self
            .client()
            .transition_sandbox(self.request(pb::TransitionSandboxRequest {
                cluster_id: self.cluster_id.to_string(),
                sandbox_id: sandbox_id.to_string(),
                node_id: node_id.to_string(),
                kind: kind as i32,
                expect_generation,
                metadata_json,
                lease_ttl_millis,
                snapshot_id,
            }))
            .await
            .map_err(|status| Self::unreachable(operation, status))?;

        Ok(response.into_inner())
    }
}

/// Decodes one timestamp column.
///
/// 🔴 Microseconds because that is what both sides can represent exactly: the
/// database stores `timestamptz` to the microsecond and the node's clock type
/// carries nanoseconds, so anything finer round-trips a write as a silently
/// truncated read.
fn decode_micros(
    sandbox_id: &str,
    micros: i64,
    column: &'static str,
) -> RegistryResult<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros).ok_or_else(|| {
        CentralPausedSandboxRegistry::invalid(
            sandbox_id,
            format!("{column} is not a representable timestamp: {micros}"),
        )
    })
}

#[async_trait]
impl PausedSandboxRegistry for CentralPausedSandboxRegistry {
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
        // The write path always has the record, and the row is what a resume on
        // another node rebuilds the sandbox from — so refuse rather than write
        // a row that promises a rebuild it cannot deliver.
        let Some(metadata) = entry.metadata.as_ref() else {
            return Err(PausedRegistryError::InvalidRecord {
                sandbox_id: entry.sandbox_id.to_string(),
                reason: "sandbox metadata is missing".to_string(),
                source: None,
            });
        };
        // Serialised here and passed through untouched from here on. Nothing
        // between this line and the JSONB column parses it, which is what keeps
        // a field this build writes and the controller has never heard of from
        // being dropped on the way past.
        let metadata_json =
            serde_json::to_vec(metadata).map_err(|e| PausedRegistryError::InvalidRecord {
                sandbox_id: entry.sandbox_id.to_string(),
                reason: "sandbox metadata is not serializable".to_string(),
                source: Some(e.into()),
            })?;

        // The node named here is the one whose disk holds the artifacts, which
        // is what `origin_node_id` means — not necessarily whichever process
        // happens to be asking.
        let response = self
            .transition(
                "begin_pause",
                pb::TransitionKind::BeginPause,
                &entry.sandbox_id,
                &entry.origin_node_id,
                None,
                metadata_json,
                String::new(),
            )
            .await?;

        let previous_snapshot_id = if response.previous_snapshot_id.is_empty() {
            None
        } else {
            Some(SnapshotId::from_uuid(
                Uuid::parse_str(&response.previous_snapshot_id).map_err(|_| {
                    Self::invalid(
                        &entry.sandbox_id.to_string(),
                        "superseded snapshot id is not a uuid".to_string(),
                    )
                })?,
            ))
        };

        debug!(sandbox_id = %entry.sandbox_id, generation = response.generation, "registered paused sandbox");

        Ok(BeganPause {
            generation: response.generation,
            previous_snapshot_id,
        })
    }

    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &SnapshotId,
    ) -> RegistryResult<()> {
        self.transition(
            "complete_pause",
            pb::TransitionKind::CompletePause,
            sandbox_id,
            &self.node_id,
            Some(generation),
            Vec::new(),
            snapshot_id.to_string(),
        )
        .await?;

        debug!(%sandbox_id, %snapshot_id, "paused sandbox is durable");

        Ok(())
    }

    async fn mark_local_only(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()> {
        self.transition(
            "mark_local_only",
            pb::TransitionKind::MarkLocalOnly,
            sandbox_id,
            &self.node_id,
            Some(generation),
            Vec::new(),
            String::new(),
        )
        .await?;

        Ok(())
    }

    async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        // Keyed by the id the row carries rather than by the one that was
        // asked for, so an answer about some other sandbox reads as "no row"
        // instead of being mistaken for this one's.
        Ok(self
            .fetch(std::slice::from_ref(sandbox_id), "get")
            .await?
            .remove(sandbox_id))
    }

    async fn get_many(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
        self.fetch(sandbox_ids, "get_many").await
    }

    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> RegistryResult<ResumeClaim> {
        let response = self
            .client()
            .acquire_sandbox(self.request(pb::AcquireSandboxRequest {
                cluster_id: self.cluster_id.to_string(),
                sandbox_id: sandbox_id.to_string(),
                node_id: node_id.to_string(),
                lease_ttl_millis: self.lease_ttl_millis,
            }))
            .await
            .map_err(|status| Self::unreachable("claim_for_resume", status))?
            .into_inner();

        // 🔴 An unset outcome is a controller that did not answer, and the
        // cheapest thing to mistake it for — `NotFound` — is read as "the
        // cluster does not track this sandbox", which lets the resume run
        // without any cluster check at all.
        let outcome = response
            .outcome
            .ok_or_else(|| Self::malformed("claim_for_resume", "a claim with no outcome"))?;

        match outcome {
            pb::acquire_sandbox_response::Outcome::Claimed(claimed) => {
                let row = claimed
                    .entry
                    .ok_or_else(|| Self::malformed("claim_for_resume", "a claim with no row"))?;
                let metadata: SandboxMetadata = serde_json::from_slice(&claimed.metadata_json)
                    .map_err(|e| PausedRegistryError::InvalidRecord {
                        sandbox_id: sandbox_id.to_string(),
                        reason: "metadata is not a sandbox record".to_string(),
                        source: Some(e.into()),
                    })?;
                let entry = self.decode_entry(row, Some(metadata))?;
                // 🔴 The state the claim replaced, taken from the field that
                // carries it. Reading it off the entry would say `resuming`
                // every time, which is what made every ordinary resume look
                // like a lease takeover for months.
                let previous_state = PausedRegistryState::parse(&claimed.previous_state)
                    .ok_or_else(|| {
                        Self::invalid(
                            &sandbox_id.to_string(),
                            format!("unknown previous state '{}'", claimed.previous_state),
                        )
                    })?;

                log_claim_outcome(sandbox_id, node_id, &entry, previous_state);

                Ok(ResumeClaim::Claimed {
                    entry: Box::new(entry),
                    previous_state,
                })
            }
            pb::acquire_sandbox_response::Outcome::NotFound(_) => Ok(ResumeClaim::NotFound),
            pb::acquire_sandbox_response::Outcome::NotReady(origin) => Ok(ResumeClaim::NotReady {
                origin_node_id: origin.origin_node_id,
            }),
            pb::acquire_sandbox_response::Outcome::Conflict(origin) => {
                // An unset reason is an older controller. It stays Unspecified
                // rather than being guessed at: every consumer today blocks on
                // any conflict, and inventing a reason here would be inventing
                // the one fact this field exists to carry honestly.
                let reason = match origin.reason() {
                    pb::ConflictReason::LiveElsewhere => ConflictReason::LiveElsewhere,
                    pb::ConflictReason::ClaimLost => ConflictReason::ClaimLost,
                    pb::ConflictReason::Unspecified => ConflictReason::Unspecified,
                };

                Ok(ResumeClaim::Conflict {
                    origin_node_id: origin.origin_node_id,
                    reason,
                })
            }
        }
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        let response = self
            .transition(
                "release_claim",
                pb::TransitionKind::ReleaseClaim,
                sandbox_id,
                &self.node_id,
                Some(generation),
                Vec::new(),
                String::new(),
            )
            .await?;

        Ok(response.matched)
    }

    async fn renew_lease(&self, node_id: &str, held: &[HeldSandbox]) -> RegistryResult<u64> {
        if held.is_empty() {
            return Ok(0);
        }

        let response = self
            .client()
            .renew_node_lease(
                self.request(pb::RenewNodeLeaseRequest {
                    cluster_id: self.cluster_id.to_string(),
                    node_id: node_id.to_string(),
                    reconcile_interval_millis: self.reconcile_interval_millis,
                    held: held
                        .iter()
                        .map(|sandbox| pb::HeldSandbox {
                            sandbox_id: sandbox.sandbox_id.to_string(),
                            // Absent means the sandbox was asked never to expire,
                            // which reclamation leaves alone forever — not the same
                            // as a deadline nobody knows.
                            expires_at_unix_micros: sandbox
                                .expires_at
                                .map(|deadline| deadline.timestamp_micros()),
                        })
                        .collect(),
                    lease_ttl_millis: self.lease_ttl_millis,
                }),
            )
            .await
            .map_err(|status| Self::unreachable("renew_lease", status))?
            .into_inner();

        debug!(
            node_id,
            renewed = response.renewed,
            "renewed paused registry leases"
        );

        Ok(response.renewed)
    }

    /// Nothing, deliberately.
    ///
    /// This is the one pass that was never about the node running it — it
    /// sweeps the whole cluster for rows whose holder stopped renewing and
    /// whose sandbox has since outlived its own deadline, and any node could
    /// run it. Once a controller owns the database it owns this too, on its own
    /// timer, so a node asking for it would only be asking the controller to do
    /// what it is already doing.
    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
        debug!(
            "skipping expired-holding reclamation: the controller runs it cluster-wide on its own timer"
        );

        Ok(ReclaimedHoldings::default())
    }

    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> RegistryResult<MarkRunningOutcome> {
        let response = self
            .transition(
                "mark_running",
                pb::TransitionKind::MarkRunning,
                sandbox_id,
                node_id,
                None,
                Vec::new(),
                String::new(),
            )
            .await?;

        // The enum is authoritative where it is set; `tracked` is what an older
        // controller answers with, and it can only distinguish adopted from
        // "one of the two others".
        Ok(match response.mark_running_outcome() {
            pb::MarkRunningOutcome::Adopted => MarkRunningOutcome::Adopted,
            pb::MarkRunningOutcome::Untracked => MarkRunningOutcome::Untracked,
            pb::MarkRunningOutcome::HeldElsewhere => MarkRunningOutcome::HeldElsewhere,
            pb::MarkRunningOutcome::Unspecified if response.tracked => MarkRunningOutcome::Adopted,
            pb::MarkRunningOutcome::Unspecified => MarkRunningOutcome::Untracked,
        })
    }

    async fn release_node_holdings(&self, node_id: &str) -> RegistryResult<ReleasedHoldings> {
        let response = self
            .client()
            .release_node_holdings(self.request(pb::ReleaseNodeHoldingsRequest {
                cluster_id: self.cluster_id.to_string(),
                node_id: node_id.to_string(),
            }))
            .await
            .map_err(|status| Self::unreachable("release_node_holdings", status))?
            .into_inner();

        Ok(ReleasedHoldings {
            released: response.released,
            discarded: response.discarded,
        })
    }

    async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        let response = self
            .transition(
                "remove",
                pb::TransitionKind::Remove,
                sandbox_id,
                &self.node_id,
                Some(generation),
                Vec::new(),
                String::new(),
            )
            .await?;

        Ok(response.removed)
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;

    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use super::*;

    const NODE: &str = "node-a";
    const OTHER: &str = "node-b";

    /// A controller that answers whatever the test told it to and remembers
    /// what it was asked.
    ///
    /// Programmable per method rather than per call: what is under test is how
    /// one outcome is read, and a queue of them would only make the failures
    /// harder to place.
    #[derive(Default)]
    struct FakeController {
        get: Mutex<Vec<Result<Vec<pb::RegistryEntry>, Status>>>,
        transition: Mutex<Option<Result<pb::TransitionSandboxResponse, Status>>>,
        acquire: Mutex<Option<Result<pb::AcquireSandboxResponse, Status>>>,
        renew: Mutex<Option<Result<pb::RenewNodeLeaseResponse, Status>>>,
        release: Mutex<Option<Result<pb::ReleaseNodeHoldingsResponse, Status>>>,
        /// Held before answering, to let a test outlive its own call budget.
        stall: Mutex<Option<Duration>>,
        seen_get: Mutex<Vec<pb::GetSandboxesRequest>>,
        seen_transition: Mutex<Vec<pb::TransitionSandboxRequest>>,
        seen_acquire: Mutex<Vec<pb::AcquireSandboxRequest>>,
        seen_renew: Mutex<Vec<pb::RenewNodeLeaseRequest>>,
        seen_release: Mutex<Vec<pb::ReleaseNodeHoldingsRequest>>,
        /// Whether every call so far carried a deadline.
        deadlines: Mutex<Vec<bool>>,
        /// Replaces the coverage a batch answer reports, so a test can produce
        /// the one failure the coverage check exists for: an answer that looks
        /// exactly like "those sandboxes have no rows" but is really "those
        /// sandboxes were never looked up".
        coverage: Mutex<Option<Vec<String>>>,
    }

    impl FakeController {
        /// Answers `GetSandboxes` with this, once per call, in order. The last
        /// answer repeats.
        fn answer_get(&self, answers: Vec<Result<Vec<pb::RegistryEntry>, Status>>) {
            *self.get.lock().unwrap() = answers;
        }

        fn note_deadline<T>(&self, request: &Request<T>) {
            self.deadlines
                .lock()
                .unwrap()
                .push(request.metadata().get("grpc-timeout").is_some());
        }

        async fn stall(&self) {
            let stall = *self.stall.lock().unwrap();
            if let Some(stall) = stall {
                tokio::time::sleep(stall).await;
            }
        }
    }

    #[async_trait]
    impl pb::paused_registry_server::PausedRegistry for Arc<FakeController> {
        async fn get_sandboxes(
            &self,
            request: Request<pb::GetSandboxesRequest>,
        ) -> Result<Response<pb::GetSandboxesResponse>, Status> {
            self.note_deadline(&request);
            self.stall().await;
            self.seen_get.lock().unwrap().push(request.into_inner());

            let answer = {
                let mut answers = self.get.lock().unwrap();
                if answers.len() > 1 {
                    answers.remove(0)
                } else {
                    answers.first().cloned().unwrap_or_else(|| Ok(Vec::new()))
                }
            };

            // The fake reports full coverage of whatever it was asked for.
            // Tests that need the opposite build the response themselves — the
            // point of the coverage check is that a *short* answer is caught,
            // and a fake that quietly under-reports would make every other test
            // here exercise the failure path instead.
            let requested = match self.coverage.lock().unwrap().clone() {
                Some(override_ids) => override_ids,
                None => self
                    .seen_get
                    .lock()
                    .unwrap()
                    .last()
                    .map(|req: &pb::GetSandboxesRequest| req.sandbox_ids.clone())
                    .unwrap_or_default(),
            };

            answer.map(|sandboxes| {
                Response::new(pb::GetSandboxesResponse {
                    sandboxes,
                    covered_sandbox_ids: requested,
                    now_unix_micros: 0,
                })
            })
        }

        async fn transition_sandbox(
            &self,
            request: Request<pb::TransitionSandboxRequest>,
        ) -> Result<Response<pb::TransitionSandboxResponse>, Status> {
            self.note_deadline(&request);
            self.stall().await;
            self.seen_transition
                .lock()
                .unwrap()
                .push(request.into_inner());

            self.transition
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Ok(pb::TransitionSandboxResponse::default()))
                .map(Response::new)
        }

        async fn acquire_sandbox(
            &self,
            request: Request<pb::AcquireSandboxRequest>,
        ) -> Result<Response<pb::AcquireSandboxResponse>, Status> {
            self.note_deadline(&request);
            self.stall().await;
            self.seen_acquire.lock().unwrap().push(request.into_inner());

            self.acquire
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Ok(pb::AcquireSandboxResponse::default()))
                .map(Response::new)
        }

        async fn renew_node_lease(
            &self,
            request: Request<pb::RenewNodeLeaseRequest>,
        ) -> Result<Response<pb::RenewNodeLeaseResponse>, Status> {
            self.note_deadline(&request);
            self.stall().await;
            self.seen_renew.lock().unwrap().push(request.into_inner());

            self.renew
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Ok(pb::RenewNodeLeaseResponse::default()))
                .map(Response::new)
        }

        async fn release_node_holdings(
            &self,
            request: Request<pb::ReleaseNodeHoldingsRequest>,
        ) -> Result<Response<pb::ReleaseNodeHoldingsResponse>, Status> {
            self.note_deadline(&request);
            self.stall().await;
            self.seen_release.lock().unwrap().push(request.into_inner());

            self.release
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Ok(pb::ReleaseNodeHoldingsResponse::default()))
                .map(Response::new)
        }
    }

    /// A registry talking to a fake controller over a real socket.
    struct Harness {
        registry: CentralPausedSandboxRegistry,
        fake: Arc<FakeController>,
        cluster_id: Uuid,
        _server: tokio::task::JoinHandle<()>,
    }

    async fn harness() -> Harness {
        let cluster_id = Uuid::new_v4();
        let fake = Arc::new(FakeController::default());
        let incoming = tonic::transport::server::TcpIncoming::bind(
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .expect("bind a port for the fake controller");
        let addr = incoming.local_addr().expect("read the bound port");

        let served = Arc::clone(&fake);
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(pb::paused_registry_server::PausedRegistryServer::new(
                    served,
                ))
                .serve_with_incoming(incoming)
                .await;
        });

        let registry = CentralPausedSandboxRegistry::connect_lazy(
            &format!("http://{addr}"),
            cluster_id,
            NODE.to_string(),
            90,
            30,
        )
        .expect("build the registry")
        .with_call_timeout(Duration::from_secs(5));

        Harness {
            registry,
            fake,
            cluster_id,
            _server: server,
        }
    }

    /// A registry pointed at a port nothing is listening on.
    ///
    /// Port 1 is privileged and unused, so this refuses immediately rather than
    /// racing a port that some other test might bind.
    fn unreachable_registry() -> CentralPausedSandboxRegistry {
        CentralPausedSandboxRegistry::connect_lazy(
            "http://127.0.0.1:1",
            Uuid::new_v4(),
            NODE.to_string(),
            90,
            30,
        )
        .expect("build the registry")
    }

    /// A listener that accepts the connection and then hangs up.
    ///
    /// A different failure from a refused connection, and it reaches the client
    /// by a different route: the socket is established and the request is sent,
    /// and what comes back is the stream ending. That is what a controller
    /// killed mid-call looks like from this side, and no status the controller
    /// could have chosen is involved.
    async fn a_registry_that_hangs_up(
    ) -> (CentralPausedSandboxRegistry, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port to hang up on");
        let addr = listener.local_addr().expect("read the bound port");
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let registry = CentralPausedSandboxRegistry::connect_lazy(
            &format!("http://{addr}"),
            Uuid::new_v4(),
            NODE.to_string(),
            90,
            30,
        )
        .expect("build the registry");

        (registry, server)
    }

    fn row(cluster_id: Uuid, sandbox_id: SandboxId, state: &str) -> pb::RegistryEntry {
        pb::RegistryEntry {
            sandbox_id: sandbox_id.to_string(),
            cluster_id: cluster_id.to_string(),
            state: state.to_string(),
            generation: 7,
            origin_node_id: NODE.to_string(),
            claimed_by_node_id: String::new(),
            snapshot_id: SnapshotId::generate().to_string(),
            paused_at_unix_micros: 1_755_561_600_123_456,
            updated_at_unix_micros: 1_755_561_700_654_321,
        }
    }

    fn entry_for(sandbox_id: SandboxId, metadata: SandboxMetadata) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: Uuid::nil(),
            state: PausedRegistryState::Publishing,
            generation: 0,
            origin_node_id: OTHER.to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: Some(metadata),
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Every gRPC status code, including the ones whose names sound like
    /// answers.
    fn every_status_code() -> Vec<tonic::Code> {
        vec![
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
            tonic::Code::InvalidArgument,
            tonic::Code::DeadlineExceeded,
            tonic::Code::NotFound,
            tonic::Code::AlreadyExists,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::FailedPrecondition,
            tonic::Code::Aborted,
            tonic::Code::OutOfRange,
            tonic::Code::Unimplemented,
            tonic::Code::Internal,
            tonic::Code::Unavailable,
            tonic::Code::DataLoss,
            tonic::Code::Unauthenticated,
        ]
    }

    /// 🔴 The whole reason this backend is written the way it is. A sandbox
    /// missing from a batch read means its row is gone, and both consumers of
    /// that answer delete things — local paused artifacts, and running
    /// sandboxes. So no failure of any kind may arrive as a short map.
    #[tokio::test]
    async fn no_grpc_failure_ever_arrives_as_an_empty_batch() {
        let harness = harness().await;
        let ids = vec![SandboxId::new()];

        for code in every_status_code() {
            harness
                .fake
                .answer_get(vec![Err(Status::new(code, "the controller said no"))]);

            let answer = harness.registry.get_many(&ids).await;

            assert!(
                matches!(answer, Err(PausedRegistryError::Backend { .. })),
                "{code:?} must be a backend failure, got {:?}",
                answer.map(|rows| rows.len())
            );
        }
    }

    /// The single read has to fail the same way: it feeds the decision about
    /// whether a local copy has been superseded, and a `None` there reads as
    /// "the cluster has moved past this sandbox".
    #[tokio::test]
    async fn no_grpc_failure_ever_arrives_as_a_missing_row() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        for code in every_status_code() {
            harness
                .fake
                .answer_get(vec![Err(Status::new(code, "the controller said no"))]);

            let answer = harness.registry.get(&sandbox_id).await;

            assert!(
                matches!(answer, Err(PausedRegistryError::Backend { .. })),
                "{code:?} must be a backend failure, got {:?}",
                answer.map(|row| row.is_some())
            );
        }
    }

    /// A controller that cannot be dialled at all — the shape of a rolling
    /// restart from the node's side.
    #[tokio::test]
    async fn a_controller_that_cannot_be_dialled_is_a_failure_not_an_answer() {
        let registry = unreachable_registry();
        let sandbox_id = SandboxId::new();

        assert!(matches!(
            registry.get_many(&[sandbox_id]).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.get(&sandbox_id).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.claim_for_resume(&sandbox_id, NODE).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.renew_lease(NODE, &[held(sandbox_id)]).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.release_node_holdings(NODE).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.remove(&sandbox_id, 1).await,
            Err(PausedRegistryError::Backend { .. })
        ));
    }

    /// A connection that is established and then ends without an answer.
    ///
    /// 🔴 Called out separately from a refused connection because it is the one
    /// a rolling restart actually produces: the controller was there when the
    /// call went out and gone before it came back, so the request may well have
    /// been executed. Nothing about that is an empty registry.
    #[tokio::test]
    async fn a_controller_that_hangs_up_mid_call_is_a_failure_not_an_answer() {
        let (registry, server) = a_registry_that_hangs_up().await;
        let sandbox_id = SandboxId::new();

        // 🔴 This arrives as `Cancelled` — a code whose name reads as "the
        // caller gave up", when in fact the request may well have been executed
        // and only the answer was lost.
        assert!(matches!(
            registry.get_many(&[sandbox_id]).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.get(&sandbox_id).await,
            Err(PausedRegistryError::Backend { .. })
        ));
        assert!(matches!(
            registry.claim_for_resume(&sandbox_id, NODE).await,
            Err(PausedRegistryError::Backend { .. })
        ));

        server.abort();
    }

    /// An answer that arrives whole and will not decode.
    ///
    /// The client refuses a message past its size limit, which is a decode
    /// failure rather than anything the controller reported — the one class of
    /// failure that arrives with a complete, successful-looking response
    /// behind it. A roster large enough to hit that limit is exactly when a
    /// node has the most local artifacts to lose.
    #[tokio::test]
    async fn an_answer_too_large_to_decode_is_a_failure_not_an_empty_registry() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        let mut oversized = row(harness.cluster_id, sandbox_id, "paused");
        // Past the client's decoding limit, which tonic defaults to 4 MiB.
        oversized.origin_node_id = "n".repeat(5 * 1024 * 1024);
        harness.fake.answer_get(vec![Ok(vec![oversized])]);

        let answer = harness.registry.get_many(&[sandbox_id]).await;

        assert!(
            matches!(answer, Err(PausedRegistryError::Backend { .. })),
            "an undecodable answer must not read as an empty registry, got {:?}",
            answer.map(|rows| rows.len())
        );
    }

    fn held(sandbox_id: SandboxId) -> HeldSandbox {
        HeldSandbox {
            sandbox_id,
            expires_at: None,
        }
    }

    /// A controller that accepts the call and then never answers.
    #[tokio::test]
    async fn a_call_that_outlives_its_budget_is_a_failure_not_an_answer() {
        let harness = harness().await;
        let registry = CentralPausedSandboxRegistry::over_channel(
            harness.registry.channel.clone(),
            harness.cluster_id,
            NODE.to_string(),
            90,
            30,
        )
        .with_call_timeout(Duration::from_millis(100));
        *harness.fake.stall.lock().unwrap() = Some(Duration::from_secs(30));

        let answer = registry.get_many(&[SandboxId::new()]).await;

        assert!(
            matches!(answer, Err(PausedRegistryError::Backend { .. })),
            "a call that ran out of time must not read as an empty registry"
        );
    }

    /// The budget has to be on the wire, not only on this side: a controller
    /// that keeps working on a request its caller has given up on holds a
    /// database connection for nothing.
    #[tokio::test]
    async fn every_call_carries_its_deadline() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        let _ = harness.registry.get(&sandbox_id).await;
        let _ = harness.registry.remove(&sandbox_id, 1).await;
        let _ = harness.registry.claim_for_resume(&sandbox_id, NODE).await;
        let _ = harness
            .registry
            .renew_lease(NODE, &[held(sandbox_id)])
            .await;
        let _ = harness.registry.release_node_holdings(NODE).await;

        let deadlines = harness.fake.deadlines.lock().unwrap().clone();
        assert_eq!(deadlines.len(), 5);
        assert!(
            deadlines.iter().all(|carried| *carried),
            "every call must carry a grpc-timeout"
        );
    }

    /// 🔴 An unset outcome is a controller that did not answer. The cheapest
    /// thing to mistake it for is `NotFound`, which arbitration reads as "the
    /// cluster does not track this sandbox" and lets the resume run with no
    /// cluster check at all.
    #[tokio::test]
    async fn a_claim_with_no_outcome_is_a_failure_not_a_missing_sandbox() {
        let harness = harness().await;
        *harness.fake.acquire.lock().unwrap() =
            Some(Ok(pb::AcquireSandboxResponse { outcome: None }));

        let answer = harness
            .registry
            .claim_for_resume(&SandboxId::new(), NODE)
            .await;

        assert!(
            matches!(answer, Err(PausedRegistryError::Backend { .. })),
            "an outcome-less claim must not read as NotFound"
        );
    }

    /// Same reasoning one level down: a claim granted without the row it was
    /// granted on cannot be acted on, and the caller must not be told it owns
    /// something it cannot rebuild.
    #[tokio::test]
    async fn a_granted_claim_with_no_row_is_a_failure() {
        let harness = harness().await;
        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::Claimed(
                pb::AcquiredSandbox {
                    entry: None,
                    metadata_json: serde_json::to_vec(&SandboxMetadata::default()).unwrap(),
                    previous_state: "paused".to_string(),
                },
            )),
        }));

        assert!(matches!(
            harness
                .registry
                .claim_for_resume(&SandboxId::new(), NODE)
                .await,
            Err(PausedRegistryError::Backend { .. })
        ));
    }

    #[tokio::test]
    async fn the_three_refusals_keep_their_distinctions() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::NotFound(
                pb::AcquireNotFound {},
            )),
        }));
        assert!(matches!(
            harness.registry.claim_for_resume(&sandbox_id, NODE).await,
            Ok(ResumeClaim::NotFound)
        ));

        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::NotReady(
                pb::AcquireOriginRef {
                    origin_node_id: OTHER.to_string(),
                    reason: pb::ConflictReason::Unspecified as i32,
                },
            )),
        }));
        assert!(matches!(
            harness.registry.claim_for_resume(&sandbox_id, NODE).await,
            Ok(ResumeClaim::NotReady { origin_node_id }) if origin_node_id == OTHER
        ));

        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::Conflict(
                pb::AcquireOriginRef {
                    origin_node_id: OTHER.to_string(),
                    reason: pb::ConflictReason::LiveElsewhere as i32,
                },
            )),
        }));
        assert!(matches!(
            harness.registry.claim_for_resume(&sandbox_id, NODE).await,
            Ok(ResumeClaim::Conflict { origin_node_id, reason })
                if origin_node_id == OTHER && reason == ConflictReason::LiveElsewhere
        ));
    }

    /// 🔴 The state a claim replaced comes from the field that carries it, not
    /// from the row it hands back — the row describes the sandbox *after* the
    /// claim, so its state is `resuming` whatever it was a moment earlier.
    /// Reading it there is what reported every ordinary resume as a lease
    /// takeover for months.
    #[tokio::test]
    async fn the_state_a_claim_replaced_is_not_read_off_the_row() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::Claimed(
                pb::AcquiredSandbox {
                    entry: Some(row(harness.cluster_id, sandbox_id, "resuming")),
                    metadata_json: serde_json::to_vec(&SandboxMetadata::default()).unwrap(),
                    previous_state: "local_only".to_string(),
                },
            )),
        }));

        let claim = harness
            .registry
            .claim_for_resume(&sandbox_id, NODE)
            .await
            .expect("the claim should be granted");

        let ResumeClaim::Claimed {
            entry,
            previous_state,
        } = claim
        else {
            panic!("expected a granted claim");
        };
        assert_eq!(entry.state, PausedRegistryState::Resuming);
        assert_eq!(previous_state, PausedRegistryState::LocalOnly);
    }

    /// The record travels as opaque bytes in both directions. Nothing between
    /// this node and the JSONB column parses it, which is what keeps a field
    /// the controller has never heard of from being dropped on the way past.
    #[tokio::test]
    async fn the_sandbox_record_travels_byte_for_byte() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        let metadata = SandboxMetadata {
            id: sandbox_id,
            snapshot_alias: Some("tpl-node22".to_string()),
            secure: true,
            ..Default::default()
        };
        let expected = serde_json::to_vec(&metadata).unwrap();

        harness
            .registry
            .begin_pause(&entry_for(sandbox_id, metadata.clone()))
            .await
            .expect("begin_pause should land");

        let sent = harness.fake.seen_transition.lock().unwrap()[0]
            .metadata_json
            .clone();
        assert_eq!(sent, expected, "the record must go out unmodified");

        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::Claimed(
                pb::AcquiredSandbox {
                    entry: Some(row(harness.cluster_id, sandbox_id, "resuming")),
                    metadata_json: sent,
                    previous_state: "paused".to_string(),
                },
            )),
        }));

        let ResumeClaim::Claimed { entry, .. } = harness
            .registry
            .claim_for_resume(&sandbox_id, NODE)
            .await
            .expect("the claim should be granted")
        else {
            panic!("expected a granted claim");
        };
        let returned = entry.metadata.expect("a claim carries the record");
        assert_eq!(
            serde_json::to_vec(&returned).unwrap(),
            expected,
            "the record must come back unmodified"
        );
    }

    /// A record the claim carries but cannot be read is one unreadable
    /// sandbox, and has to be reported as one rather than as a granted claim
    /// over a default record — which would rebuild something that is not the
    /// sandbox that was asked for.
    #[tokio::test]
    async fn a_claim_carrying_an_unreadable_record_is_refused() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
            outcome: Some(pb::acquire_sandbox_response::Outcome::Claimed(
                pb::AcquiredSandbox {
                    entry: Some(row(harness.cluster_id, sandbox_id, "resuming")),
                    metadata_json: br#"{"not":"a sandbox"}"#.to_vec(),
                    previous_state: "paused".to_string(),
                },
            )),
        }));

        assert!(matches!(
            harness.registry.claim_for_resume(&sandbox_id, NODE).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));
    }

    /// The direct backend's read-time invariants, on the side of the wire that
    /// acts on them.
    #[tokio::test]
    async fn a_row_this_build_cannot_read_is_reported_rather_than_skipped() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        // A state from a schema this build has not heard of.
        let mut unknown_state = row(harness.cluster_id, sandbox_id, "quiescing");
        unknown_state.snapshot_id = String::new();
        harness.fake.answer_get(vec![Ok(vec![unknown_state])]);
        assert!(matches!(
            harness.registry.get(&sandbox_id).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));

        // Durable, but naming no snapshot: it promises a cross-node resume it
        // cannot deliver.
        let mut no_snapshot = row(harness.cluster_id, sandbox_id, "paused");
        no_snapshot.snapshot_id = String::new();
        harness.fake.answer_get(vec![Ok(vec![no_snapshot])]);
        assert!(matches!(
            harness.registry.get(&sandbox_id).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));

        // A timestamp no clock could produce.
        let mut bad_time = row(harness.cluster_id, sandbox_id, "paused");
        bad_time.updated_at_unix_micros = i64::MAX;
        harness.fake.answer_get(vec![Ok(vec![bad_time])]);
        assert!(matches!(
            harness.registry.get(&sandbox_id).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));
    }

    /// 🔴 The cluster scope is written into every statement the direct backend
    /// runs. Once it is something a request merely asks for, nothing else stops
    /// one cluster acting on another's rows — claiming a sandbox whose snapshot
    /// is in a repository this node cannot read, and then deleting the row as
    /// dangling.
    #[tokio::test]
    async fn a_row_belonging_to_another_cluster_is_refused() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        harness
            .fake
            .answer_get(vec![Ok(vec![row(Uuid::new_v4(), sandbox_id, "paused")])]);

        assert!(matches!(
            harness.registry.get(&sandbox_id).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));
    }

    /// An answer about some other sandbox is not an answer about this one.
    #[tokio::test]
    async fn a_row_for_a_different_sandbox_is_not_mistaken_for_this_one() {
        let harness = harness().await;
        let asked_for = SandboxId::new();
        harness.fake.answer_get(vec![Ok(vec![row(
            harness.cluster_id,
            SandboxId::new(),
            "paused",
        )])]);

        assert!(harness
            .registry
            .get(&asked_for)
            .await
            .expect("a well-formed answer")
            .is_none());
    }

    /// Absence in a successful answer is the one legitimate "no row", and it
    /// has to stay reachable — the caller acts on it.
    #[tokio::test]
    async fn an_absent_row_in_a_successful_answer_is_still_no_row() {
        let harness = harness().await;
        harness.fake.answer_get(vec![Ok(Vec::new())]);

        assert!(harness
            .registry
            .get(&SandboxId::new())
            .await
            .expect("a well-formed answer")
            .is_none());
        assert!(harness
            .registry
            .get_many(&[SandboxId::new()])
            .await
            .expect("a well-formed answer")
            .is_empty());
    }

    /// Asking about nothing asks nothing, so an idle node makes no calls at
    /// all.
    #[tokio::test]
    async fn asking_about_nothing_asks_the_controller_nothing() {
        let harness = harness().await;

        assert!(harness.registry.get_many(&[]).await.unwrap().is_empty());
        assert_eq!(harness.registry.renew_lease(NODE, &[]).await.unwrap(), 0);

        assert!(harness.fake.seen_get.lock().unwrap().is_empty());
        assert!(harness.fake.seen_renew.lock().unwrap().is_empty());
    }

    /// 🔴 Reclamation is the one pass that was never about the node running it.
    /// The controller owns the database and runs it on its own timer, so a node
    /// asking for it would be asking for work already being done — and a node
    /// that reported having done some would be reporting the controller's work
    /// as its own.
    #[tokio::test]
    async fn reclaiming_expired_holdings_asks_the_controller_nothing() {
        let harness = harness().await;

        let reclaimed = harness
            .registry
            .reclaim_expired_holdings()
            .await
            .expect("a local no-op");

        assert!(reclaimed.is_empty());
        assert!(harness.fake.seen_get.lock().unwrap().is_empty());
        assert!(harness.fake.seen_transition.lock().unwrap().is_empty());
        assert!(harness.fake.seen_release.lock().unwrap().is_empty());
    }

    /// One RPC covers six operations, so which one it is and whether it is
    /// conditional both have to travel with it. Getting either wrong turns a
    /// guarded write into an unguarded one.
    #[tokio::test]
    async fn every_transition_names_itself_and_says_whether_it_is_conditional() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::generate();

        harness
            .registry
            .begin_pause(&entry_for(sandbox_id, SandboxMetadata::default()))
            .await
            .unwrap();
        harness
            .registry
            .complete_pause(&sandbox_id, 11, &snapshot_id)
            .await
            .unwrap();
        harness
            .registry
            .mark_local_only(&sandbox_id, 12)
            .await
            .unwrap();
        harness
            .registry
            .mark_running(&sandbox_id, OTHER)
            .await
            .unwrap();
        harness
            .registry
            .release_claim(&sandbox_id, 13)
            .await
            .unwrap();
        harness.registry.remove(&sandbox_id, 21).await.unwrap();

        let seen = harness.fake.seen_transition.lock().unwrap().clone();
        let kinds: Vec<(i32, Option<i64>)> = seen
            .iter()
            .map(|request| (request.kind, request.expect_generation))
            .collect();

        assert_eq!(
            kinds,
            vec![
                (pb::TransitionKind::BeginPause as i32, None),
                (pb::TransitionKind::CompletePause as i32, Some(11)),
                (pb::TransitionKind::MarkLocalOnly as i32, Some(12)),
                (pb::TransitionKind::MarkRunning as i32, None),
                (pb::TransitionKind::ReleaseClaim as i32, Some(13)),
                // 🔴 Remove quotes one too, since D11. It was the interface's
                // one unconditional destructive write; the guard used to be the
                // caller reading the row first and deciding for itself.
                (pb::TransitionKind::Remove as i32, Some(21)),
            ]
        );
        assert_eq!(seen[1].snapshot_id, snapshot_id.to_string());
        // The claim guard is by node, so the node a resume names has to be the
        // one that reaches the row rather than whoever the process calls itself.
        assert_eq!(seen[3].node_id, OTHER);
        // A pause names the node whose disk holds the artifacts.
        assert_eq!(seen[0].node_id, OTHER);
        assert!(seen
            .iter()
            .all(|request| request.cluster_id == harness.cluster_id.to_string()));
    }

    /// The TTL is the node's to set: its own configuration holds it to three
    /// renewal intervals, so a lease stamped to anyone else's number would
    /// expire under a node renewing exactly as it was told to.
    ///
    /// Every call that stamps a lease carries it, and the one that does not
    /// stamp one does not — `remove` deletes the row, so a TTL there would
    /// describe a lease nothing is left to hold.
    #[tokio::test]
    async fn the_lease_length_travels_with_every_write() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::generate();

        harness
            .registry
            .begin_pause(&entry_for(sandbox_id, SandboxMetadata::default()))
            .await
            .unwrap();
        harness
            .registry
            .complete_pause(&sandbox_id, 1, &snapshot_id)
            .await
            .unwrap();
        harness
            .registry
            .mark_local_only(&sandbox_id, 2)
            .await
            .unwrap();
        harness
            .registry
            .mark_running(&sandbox_id, NODE)
            .await
            .unwrap();
        harness
            .registry
            .release_claim(&sandbox_id, 3)
            .await
            .unwrap();
        harness.registry.remove(&sandbox_id, 21).await.unwrap();
        harness
            .registry
            .claim_for_resume(&sandbox_id, NODE)
            .await
            .ok();
        harness
            .registry
            .renew_lease(NODE, &[held(sandbox_id)])
            .await
            .unwrap();

        let stamped: Vec<(i32, i64)> = harness
            .fake
            .seen_transition
            .lock()
            .unwrap()
            .iter()
            .map(|request| (request.kind, request.lease_ttl_millis))
            .collect();

        assert_eq!(
            stamped,
            vec![
                (pb::TransitionKind::BeginPause as i32, 90_000),
                (pb::TransitionKind::CompletePause as i32, 90_000),
                (pb::TransitionKind::MarkLocalOnly as i32, 90_000),
                (pb::TransitionKind::MarkRunning as i32, 90_000),
                (pb::TransitionKind::ReleaseClaim as i32, 90_000),
                // 🔴 Not carried: see the transition helper.
                (pb::TransitionKind::Remove as i32, 0),
            ]
        );
        assert_eq!(
            harness.fake.seen_acquire.lock().unwrap()[0].lease_ttl_millis,
            90_000
        );
        assert_eq!(
            harness.fake.seen_renew.lock().unwrap()[0].lease_ttl_millis,
            90_000
        );
    }

    /// The deadline rides along with the renewal because the two facts are only
    /// useful together, and an absent one means "asked never to expire" rather
    /// than "unknown".
    #[tokio::test]
    async fn a_renewal_carries_every_holding_and_its_deadline() {
        let harness = harness().await;
        let mortal = SandboxId::new();
        let immortal = SandboxId::new();
        let deadline = DateTime::from_timestamp_micros(1_755_562_500_000_007).unwrap();
        *harness.fake.renew.lock().unwrap() = Some(Ok(pb::RenewNodeLeaseResponse { renewed: 2 }));

        let renewed = harness
            .registry
            .renew_lease(
                NODE,
                &[
                    HeldSandbox {
                        sandbox_id: mortal,
                        expires_at: Some(deadline),
                    },
                    HeldSandbox {
                        sandbox_id: immortal,
                        expires_at: None,
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(renewed, 2);
        let sent = harness.fake.seen_renew.lock().unwrap()[0].clone();
        assert_eq!(sent.node_id, NODE);
        assert_eq!(
            sent.held,
            vec![
                pb::HeldSandbox {
                    sandbox_id: mortal.to_string(),
                    expires_at_unix_micros: Some(1_755_562_500_000_007),
                },
                pb::HeldSandbox {
                    sandbox_id: immortal.to_string(),
                    expires_at_unix_micros: None,
                },
            ]
        );
    }

    /// 🔴 Microseconds, exactly. Anything coarser makes two transitions in the
    /// same second unorderable; anything finer round-trips a write as a
    /// silently truncated read.
    #[tokio::test]
    async fn timestamps_keep_their_microseconds() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        harness.fake.answer_get(vec![Ok(vec![row(
            harness.cluster_id,
            sandbox_id,
            "paused",
        )])]);

        let entry = harness
            .registry
            .get(&sandbox_id)
            .await
            .unwrap()
            .expect("a row");

        assert_eq!(entry.paused_at.timestamp_micros(), 1_755_561_600_123_456);
        assert_eq!(entry.updated_at.timestamp_micros(), 1_755_561_700_654_321);
    }

    /// A bulk read is the reconciliation path, and it reads only the four
    /// fields that decide supersession. Carrying the record there would put a
    /// node's whole roster of them on one response for nothing.
    #[tokio::test]
    async fn a_bulk_read_carries_no_sandbox_record() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();
        let mut answered = row(harness.cluster_id, sandbox_id, "resuming");
        answered.claimed_by_node_id = OTHER.to_string();
        harness.fake.answer_get(vec![Ok(vec![answered])]);

        let rows = harness.registry.get_many(&[sandbox_id]).await.unwrap();
        let entry = rows.get(&sandbox_id).expect("a row");

        assert!(entry.metadata.is_none());
        assert_eq!(entry.state, PausedRegistryState::Resuming);
        assert_eq!(entry.claimed_by_node_id.as_deref(), Some(OTHER));
        assert_eq!(entry.origin_node_id, NODE);
        assert_eq!(entry.generation, 7);
    }

    /// A roster larger than one request is still one answer: a chunk that fails
    /// fails the call, because a partial map is a deletion instruction for
    /// whatever it left out.
    #[tokio::test]
    async fn a_roster_split_across_requests_is_still_all_or_nothing() {
        let harness = harness().await;
        let ids: Vec<SandboxId> = (0..GET_MANY_CHUNK + 5).map(|_| SandboxId::new()).collect();
        harness.fake.answer_get(vec![
            Ok(vec![row(harness.cluster_id, ids[0], "paused")]),
            Err(Status::unavailable("the second chunk did not land")),
        ]);

        let answer = harness.registry.get_many(&ids).await;

        assert!(
            matches!(answer, Err(PausedRegistryError::Backend { .. })),
            "a chunk that failed must not leave the rest looking absent"
        );
        assert_eq!(harness.fake.seen_get.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_pause_reports_the_generation_and_the_snapshot_it_superseded() {
        let harness = harness().await;
        let superseded = SnapshotId::generate();
        *harness.fake.transition.lock().unwrap() = Some(Ok(pb::TransitionSandboxResponse {
            generation: 4,
            previous_snapshot_id: superseded.to_string(),
            ..Default::default()
        }));

        let began = harness
            .registry
            .begin_pause(&entry_for(SandboxId::new(), SandboxMetadata::default()))
            .await
            .unwrap();

        assert_eq!(began.generation, 4);
        assert_eq!(began.previous_snapshot_id, Some(superseded));
    }

    /// A pause with nothing to supersede has to be told apart from one whose
    /// answer went missing — the caller deletes the snapshot this names.
    #[tokio::test]
    async fn a_first_pause_supersedes_nothing() {
        let harness = harness().await;
        *harness.fake.transition.lock().unwrap() = Some(Ok(pb::TransitionSandboxResponse {
            generation: 1,
            previous_snapshot_id: String::new(),
            ..Default::default()
        }));

        let began = harness
            .registry
            .begin_pause(&entry_for(SandboxId::new(), SandboxMetadata::default()))
            .await
            .unwrap();

        assert_eq!(began.previous_snapshot_id, None);
    }

    /// A pause whose record is missing must not write a row: the row is what a
    /// resume elsewhere rebuilds from.
    #[tokio::test]
    async fn a_pause_without_a_record_is_refused_before_it_is_sent() {
        let harness = harness().await;
        let mut entry = entry_for(SandboxId::new(), SandboxMetadata::default());
        entry.metadata = None;

        assert!(matches!(
            harness.registry.begin_pause(&entry).await,
            Err(PausedRegistryError::InvalidRecord { .. })
        ));
        assert!(harness.fake.seen_transition.lock().unwrap().is_empty());
    }

    /// 🔴 An absent record must never leave here as JSON `null`.
    ///
    /// `null` is valid JSON and the column would take it, but the record has
    /// ten fields with neither a default nor an `Option`, so a `null` row stops
    /// decoding on the way back — and every read path shares that decoder. One
    /// such row fails the whole batch read, which is the call reconciliation
    /// runs on, so a single poisoned row freezes that node's reconciliation
    /// entirely rather than affecting only its own sandbox. The controller
    /// refuses anything that is not a JSON object for the same reason; this is
    /// the near end of the same rule.
    ///
    /// Kept as a test rather than trusted to the type: the refusal above is
    /// what makes it impossible, and a later change that "helpfully" serialises
    /// the `Option` instead would compile.
    #[tokio::test]
    async fn a_pause_never_writes_a_record_that_is_not_an_object() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        harness
            .registry
            .begin_pause(&entry_for(sandbox_id, SandboxMetadata::default()))
            .await
            .unwrap();

        let sent = harness.fake.seen_transition.lock().unwrap()[0]
            .metadata_json
            .clone();
        let document: serde_json::Value =
            serde_json::from_slice(&sent).expect("the record must be JSON");

        assert!(
            document.is_object(),
            "the record must go out as a JSON object, got {document}"
        );
        assert!(
            !sent.is_empty(),
            "an empty body is how the wire says 'no record', which begin_pause may not say"
        );
    }

    /// `false` covers both "the cluster does not track this sandbox" and
    /// "someone else holds the claim", and the caller must not treat the
    /// registry as having anything to say in either case.
    #[tokio::test]
    async fn marking_a_sandbox_running_reports_whether_the_cluster_tracks_it() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        for (outcome, expected) in [
            (pb::MarkRunningOutcome::Adopted, MarkRunningOutcome::Adopted),
            (
                pb::MarkRunningOutcome::Untracked,
                MarkRunningOutcome::Untracked,
            ),
            // 🔴 The one the bool could not carry: a row exists and another
            // node holds the claim on it, which is two nodes bringing the same
            // sandbox up — not the healthy "never paused" case it used to be
            // indistinguishable from.
            (
                pb::MarkRunningOutcome::HeldElsewhere,
                MarkRunningOutcome::HeldElsewhere,
            ),
        ] {
            *harness.fake.transition.lock().unwrap() = Some(Ok(pb::TransitionSandboxResponse {
                tracked: outcome == pb::MarkRunningOutcome::Adopted,
                mark_running_outcome: outcome as i32,
                ..Default::default()
            }));

            assert_eq!(
                harness
                    .registry
                    .mark_running(&sandbox_id, NODE)
                    .await
                    .unwrap(),
                expected
            );
        }
    }

    /// The two conflicts are not interchangeable on the way in either.
    ///
    /// `ClaimLost` says the row is claimable again and a retry is legitimate;
    /// `LiveElsewhere` says the sandbox is running somewhere and retrying is
    /// how a second copy of it happens. Collapsing them here would hand every
    /// caller the more alarming of the two, or the more dangerous one.
    #[tokio::test]
    async fn each_conflict_reason_survives_the_wire() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        for (wire, expected) in [
            (
                pb::ConflictReason::LiveElsewhere,
                ConflictReason::LiveElsewhere,
            ),
            (pb::ConflictReason::ClaimLost, ConflictReason::ClaimLost),
            // An older controller says nothing, and nothing is what the node
            // records — inventing a reason here would be inventing the one
            // fact this field exists to carry honestly.
            (pb::ConflictReason::Unspecified, ConflictReason::Unspecified),
        ] {
            *harness.fake.acquire.lock().unwrap() = Some(Ok(pb::AcquireSandboxResponse {
                outcome: Some(pb::acquire_sandbox_response::Outcome::Conflict(
                    pb::AcquireOriginRef {
                        origin_node_id: OTHER.to_string(),
                        reason: wire as i32,
                    },
                )),
            }));

            match harness.registry.claim_for_resume(&sandbox_id, NODE).await {
                Ok(ResumeClaim::Conflict { reason, .. }) => assert_eq!(reason, expected),
                other => panic!("expected a conflict, got {other:?}"),
            }
        }
    }

    /// 🔴 The failure this check exists for.
    ///
    /// A batch answer that quietly covers fewer ids than it was asked about is
    /// byte-for-byte what "those sandboxes have no rows" looks like, and the
    /// node answers that by tearing down running VMs and deleting the artifacts
    /// they came from. All-or-nothing is the controller's contract, but from
    /// here it is unverifiable unless the coverage is stated — so a short answer
    /// has to fail rather than be read.
    #[tokio::test]
    async fn a_batch_answer_that_covers_less_than_it_was_asked_is_a_failure() {
        let harness = harness().await;
        let present = SandboxId::new();
        let dropped = SandboxId::new();

        harness
            .fake
            .answer_get(vec![Ok(vec![row(harness.cluster_id, present, "paused")])]);
        // The answer names only the sandbox it found. The other id is absent
        // from both lists, which is the ambiguity the check refuses to resolve
        // in the caller's favour.
        *harness.fake.coverage.lock().unwrap() = Some(vec![present.to_string()]);

        let answer = harness.registry.get_many(&[present, dropped]).await;

        assert!(
            matches!(answer, Err(PausedRegistryError::Backend { .. })),
            "an answer that did not cover every id must not be read as rows that do not exist, got {answer:?}"
        );
    }

    /// The same check must not fire on a controller that predates the field.
    ///
    /// It reports no coverage at all, and refusing that would stall every
    /// node's reconciliation for the length of a rollout — against a controller
    /// this node was already talking to without any coverage check a moment
    /// earlier.
    #[tokio::test]
    async fn a_controller_that_reports_no_coverage_is_still_read() {
        let harness = harness().await;
        let present = SandboxId::new();
        let absent = SandboxId::new();

        harness
            .fake
            .answer_get(vec![Ok(vec![row(harness.cluster_id, present, "paused")])]);
        *harness.fake.coverage.lock().unwrap() = Some(Vec::new());

        let rows = harness
            .registry
            .get_many(&[present, absent])
            .await
            .expect("an older controller's answer is still an answer");

        assert!(rows.contains_key(&present));
        assert!(!rows.contains_key(&absent));
    }

    /// A controller that predates the enum answers with the bool alone, and a
    /// node must still read it correctly rather than treating every answer as
    /// "unspecified" — during a rollout that would mean every resume on this
    /// node stops registering itself with the cluster.
    #[tokio::test]
    async fn an_older_controller_is_read_through_the_bool() {
        let harness = harness().await;
        let sandbox_id = SandboxId::new();

        for (tracked, expected) in [
            (true, MarkRunningOutcome::Adopted),
            (false, MarkRunningOutcome::Untracked),
        ] {
            *harness.fake.transition.lock().unwrap() = Some(Ok(pb::TransitionSandboxResponse {
                tracked,
                mark_running_outcome: pb::MarkRunningOutcome::Unspecified as i32,
                ..Default::default()
            }));

            assert_eq!(
                harness
                    .registry
                    .mark_running(&sandbox_id, NODE)
                    .await
                    .unwrap(),
                expected
            );
        }
    }

    /// The two numbers mean different things to an operator: released
    /// sandboxes come back on the next resume, discarded ones are gone.
    #[tokio::test]
    async fn releasing_a_predecessors_holdings_reports_both_outcomes() {
        let harness = harness().await;
        *harness.fake.release.lock().unwrap() = Some(Ok(pb::ReleaseNodeHoldingsResponse {
            released: 3,
            discarded: 2,
        }));

        let released = harness.registry.release_node_holdings(OTHER).await.unwrap();

        assert_eq!(released.released, 3);
        assert_eq!(released.discarded, 2);
        assert_eq!(harness.fake.seen_release.lock().unwrap()[0].node_id, OTHER);
    }

    /// Reconciliation refuses to run against a registry that does not track
    /// sandboxes cluster-wide, because such a registry answers "no record" for
    /// everything — which reads as "every paused sandbox has moved on".
    #[tokio::test]
    async fn the_central_registry_is_cluster_backed() {
        assert!(harness().await.registry.is_cluster_backed());
    }
}
