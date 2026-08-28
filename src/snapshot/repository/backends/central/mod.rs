//! The snapshot catalog, reached through whoever owns the database.
//!
//! The node says what it wants over gRPC and the controller runs the statement,
//! so the DSN, the connection budget and the schema stay off every machine that
//! runs user code — the same arrangement, and the same reason, as
//! [`crate::orchestrator::paused_registry::central`].
//!
//! 🔴 The reason it is *this* arrangement and not a PostgreSQL client on the
//! node is narrower than that, though, and it is the one that decided it: a
//! pause writes two facts — the sandbox is parked, and here is the snapshot it
//! parked into — and only a catalog living beside `paused_sandboxes` can put
//! them in one statement. A node holding its own connection would have two
//! sockets and the window would still be open.
//!
//! 🔴 **A failure is never an answer.** Every transport fault becomes
//! [`RepositoryError::Backend`], never an empty result: an empty `get` means
//! "no such snapshot", which callers act on by deleting artifacts and by
//! refusing a resume. A response that arrived and says nothing — an outcome
//! oneof nobody set — is a protocol failure for the same reason.
//!
//! 🔴 **What may cross this wire.** Scalar columns, and two blobs the server
//! stores without reading. `CommittedSnapshot` and `TemplateBuildErrorReason`
//! have no Go counterpart and must never grow one; see `convert.rs`.

mod convert;

use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use tonic::transport::Channel;
use tracing::debug;
use uuid::Uuid;

use crate::proto::scheduler as pb;
use crate::scheduler_endpoint::SchedulerEndpointSource;
use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotCatalog, SnapshotCommit, SnapshotCursor, SnapshotListFilter,
    SnapshotListPage, StartedBuild,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    SnapshotAlias, SnapshotId, SnapshotRecord, SnapshotSource, TemplateBuildErrorReason,
};

use convert::{
    build_status_str, decode_row, encode_build_error, encode_committed, record_source_kind,
    source_kind_str, source_sandbox_id, COMMITTED_PAYLOAD_SCHEMA,
};
pub use convert::{opening_status, STATUS_BUILDING};

/// How long any one catalog call may take.
///
/// The same budget the registry client uses, for the same reason: these calls
/// sit on publish and resume, and a request nobody has answered in ten seconds
/// has already cost more than failing it would.
const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest page this client will ask for when draining a listing.
///
/// The server caps the value anyway; asking for a round number keeps the number
/// of round trips predictable rather than depending on a server default this
/// build cannot see.
const LIST_PAGE_SIZE: u32 = 200;

/// Hard stop on how many pages one `list` will walk.
///
/// 🔴 A cursor that stops advancing would otherwise spin here forever holding a
/// request open. The bound is generous — two hundred thousand rows — and being
/// hit is a protocol failure, reported as one, not a short answer.
const LIST_PAGE_LIMIT: usize = 1_000;

/// A refusal the caller has to act on, as opposed to a failure it can only
/// report.
///
/// Kept as a Rust enum rather than folded straight into [`RepositoryError`]
/// because the double-write wrapper branches on which refusal it was: some mean
/// the request is wrong and must fail, and some mean only that the mirror is
/// behind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogRefusal {
    NotFound,
    /// The fencing predicate did not match; the row is no longer in the status
    /// this call acts from.
    StatusMismatch {
        observed: String,
    },
    AliasTaken {
        holder: String,
    },
    GenerationMismatch {
        observed: Option<i64>,
    },
    /// 🔴 Terminal. A superseded incarnation tried to write.
    ExecutionSuperseded,
    BuildInProgress {
        active_build_id: String,
    },
    BuildQueueFull,
    AlreadyExists,
    /// A reason this build has not heard of. Reported rather than treated as
    /// any of the above.
    Unknown(i32),
}

impl CatalogRefusal {
    fn from_proto(rejected: pb::CatalogRejected) -> Self {
        match pb::CatalogRejection::try_from(rejected.reason) {
            Ok(pb::CatalogRejection::NotFound) => Self::NotFound,
            Ok(pb::CatalogRejection::StatusMismatch) => Self::StatusMismatch {
                observed: rejected.observed_status,
            },
            Ok(pb::CatalogRejection::AliasTaken) => Self::AliasTaken {
                holder: rejected.alias_holder_snapshot_id,
            },
            Ok(pb::CatalogRejection::GenerationMismatch) => Self::GenerationMismatch {
                observed: rejected.observed_generation,
            },
            Ok(pb::CatalogRejection::ExecutionSuperseded) => Self::ExecutionSuperseded,
            Ok(pb::CatalogRejection::BuildInProgress) => Self::BuildInProgress {
                active_build_id: rejected.active_build_id,
            },
            Ok(pb::CatalogRejection::BuildQueueFull) => Self::BuildQueueFull,
            Ok(pb::CatalogRejection::AlreadyExists) => Self::AlreadyExists,
            _ => Self::Unknown(rejected.reason),
        }
    }

    pub fn as_metric_label(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::StatusMismatch { .. } => "status_mismatch",
            Self::AliasTaken { .. } => "alias_taken",
            Self::GenerationMismatch { .. } => "generation_mismatch",
            Self::ExecutionSuperseded => "execution_superseded",
            Self::BuildInProgress { .. } => "build_in_progress",
            Self::BuildQueueFull => "build_queue_full",
            Self::AlreadyExists => "already_exists",
            Self::Unknown(_) => "unknown",
        }
    }
}

impl std::fmt::Display for CatalogRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "no such snapshot"),
            Self::StatusMismatch { observed } => {
                write!(
                    f,
                    "the row is '{observed}', not the status this write acts from"
                )
            }
            Self::AliasTaken { holder } => write!(f, "the alias is held by '{holder}'"),
            Self::GenerationMismatch { observed } => match observed {
                Some(generation) => write!(f, "the registry row is at generation {generation}"),
                None => write!(f, "the registry row moved"),
            },
            Self::ExecutionSuperseded => write!(f, "sandbox_execution_superseded"),
            Self::BuildInProgress { active_build_id } => {
                write!(f, "build '{active_build_id}' already holds this template")
            }
            Self::BuildQueueFull => write!(f, "the cluster is at its concurrent-build ceiling"),
            Self::AlreadyExists => write!(f, "a row with this id already exists"),
            Self::Unknown(reason) => {
                write!(f, "refusal code {reason}, which this build cannot read")
            }
        }
    }
}

/// One write's outcome: the row, or the reason the catalog would not write it.
pub enum CatalogWrite<T> {
    Applied(T),
    Refused(CatalogRefusal),
}

/// gRPC-backed [`SnapshotCatalog`].
pub struct CentralSnapshotCatalog {
    endpoint_source: SchedulerEndpointSource,
    cluster_id: Uuid,
    /// This process's own identity — never a placement decision.
    ///
    /// It is the `node_id` every write carries, and — for a snapshot this
    /// node is staging — the `origin_node_id` the row records.
    ///
    /// 🔴 For `builds.node_id` specifically, "this process" is not always
    /// "the machine that runs the build". A local build (the pre-split single process) has
    /// them coincide. A build `aenv-api` dispatches to a node
    /// (`crate::node_client::build_template_on_a_node`) does not: `start_build`
    /// is called by the API replica *before* a node is even chosen — nothing
    /// this struct does learns the executor's identity, ever — and
    /// `renew_build_lease` keeps heartbeating from the same replica for the
    /// build's whole run (`hold_the_build_lease`,
    /// `src/api/impls/template.rs`). Both calls must keep sending this same
    /// value: `renewBuildLeaseSQL`'s `WHERE node_id = $2` is what lets the
    /// heartbeat reach the row it opened, and the reaper frees a build whose
    /// heartbeat has gone stale — so a `node_id` that changed between the two
    /// calls would desync them and have the reaper kill a build that is still
    /// legitimately running. This column therefore answers "who is
    /// administering this build's lease", not "which machine is running it";
    /// see `crate::node_client::build` for a log line that answers the
    /// second question.
    node_id: String,
    call_timeout: Duration,
}

impl CentralSnapshotCatalog {
    /// Builds a client pointed at `endpoint`, with no file-driven
    /// hot-reload — the static endpoint for the rest of this process's
    /// lifetime. Production assembly uses
    /// [`connect_hot_reloadable`](Self::connect_hot_reloadable) instead;
    /// this constructor stays simple for tests and any caller with no
    /// reason to reach for the other one.
    ///
    /// Lazy, like every other gRPC client here: the endpoint is parsed now and
    /// dialled on first use, so a controller that is still rolling does not
    /// stop the node from starting.
    pub fn connect_lazy(endpoint: &str, cluster_id: Uuid, node_id: String) -> anyhow::Result<Self> {
        let endpoint_source =
            SchedulerEndpointSource::spawn(endpoint.to_string(), None, "snapshot_catalog")?;
        Ok(Self::over_endpoint_source(
            endpoint_source,
            cluster_id,
            node_id,
        ))
    }

    /// [`connect_lazy`](Self::connect_lazy), but the endpoint can be
    /// hot-reloaded from `[cluster].scheduler_endpoint_file` (or its
    /// deprecated fallback) while the process runs — see
    /// [`SchedulerEndpointSource::spawn_from_config`].
    ///
    /// 🔴 No production caller since the snapshot catalog became PostgreSQL
    /// alone: `build_snapshot_backend` takes the catalog `aenv-api` builds over
    /// `[pg]` and there is no gRPC arm left to choose. What still exercises
    /// this client is `crates/aenv-node/tests/snapshot_catalog.rs` against a
    /// real `services/scheduler`.
    pub fn connect_hot_reloadable(
        endpoint: &str,
        cluster: &crate::cfg::ClusterConfig,
        scheduler_report: &crate::cfg::ObservabilitySchedulerReportConfig,
        cluster_id: Uuid,
        node_id: String,
    ) -> anyhow::Result<Self> {
        let endpoint_source = SchedulerEndpointSource::spawn_from_config(
            endpoint.to_string(),
            cluster,
            scheduler_report,
            "snapshot_catalog",
        )?;
        Ok(Self::over_endpoint_source(
            endpoint_source,
            cluster_id,
            node_id,
        ))
    }

    pub fn over_endpoint_source(
        endpoint_source: SchedulerEndpointSource,
        cluster_id: Uuid,
        node_id: String,
    ) -> Self {
        Self {
            endpoint_source,
            cluster_id,
            node_id,
            call_timeout: GRPC_CALL_TIMEOUT,
        }
    }

    pub fn with_call_timeout(mut self, call_timeout: Duration) -> Self {
        self.call_timeout = call_timeout;
        self
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    fn client(&self) -> pb::snapshot_catalog_client::SnapshotCatalogClient<Channel> {
        pb::snapshot_catalog_client::SnapshotCatalogClient::new(self.endpoint_source.channel())
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(self.call_timeout);
        request
    }

    /// The one place a transport outcome becomes a repository error.
    ///
    /// Every status code lands here, including the ones that sound like
    /// answers: `NOT_FOUND` is the controller failing to serve the call, not
    /// the snapshot being absent — absence is an empty field in a successful
    /// response. `UNAVAILABLE` while the migration has not run arrives the same
    /// way, and means the same thing to a caller: nothing was learned.
    ///
    /// 🔴 But "nothing was learned" is not true of every code, and treating it
    /// as if it were is what made a permanent rejection immortal. A status the
    /// server will produce again for the same request is not an outage: it is
    /// the server stating a rule, over a wire that has no room for a
    /// [`CatalogRefusal`]. [`Self::will_answer_the_same_way`] separates them,
    /// and the two halves become two different repository errors — which is
    /// what the double write and its compensator branch on.
    fn unreachable(operation: &'static str, status: tonic::Status) -> RepositoryError {
        let code = status.code();
        if Self::will_answer_the_same_way(code) {
            return RepositoryError::InvalidRequest {
                reason: format!(
                    "the snapshot catalog rejected '{operation}' permanently: {code}: {}",
                    status.message()
                ),
            };
        }
        RepositoryError::backend(
            format!("snapshot catalog '{operation}' failed"),
            anyhow!("{}: {}", code, status.message().to_owned()),
        )
    }

    /// Whether asking again with the same request gets the same answer.
    ///
    /// 🔴 The measured defect this exists for: the node sent every template
    /// row with `disk_size_mib = 0`, the controller refused it with
    /// `INVALID_ARGUMENT`, that became a `Backend` error, `Backend` is the one
    /// error the repair pass retries — and twenty entries were replayed every
    /// thirty seconds forever while `mirror_lag{direction="central"}` sat
    /// frozen. A number that can never reach zero is not a gate; the batch
    /// after this one is gated on exactly that number.
    ///
    /// The three codes here are the ones `catalogErrorCode` in
    /// `services/scheduler/internal/catalog_service.go` produces for a request
    /// the store will not accept: `ErrInvalidArgument` (a field is wrong),
    /// `ErrInvalidRecord` and `ErrNoPausedHalf` (`FAILED_PRECONDITION` — the
    /// comment there says it in as many words: "an operator with a psql
    /// prompt, not a retry"), and `OUT_OF_RANGE` for the same class of reason.
    ///
    /// 🔴 What is deliberately *not* here, and why:
    ///
    /// - `UNAVAILABLE` is the default arm on that side, chosen so that
    ///   whatever the controller failed at is never mistaken for the table's
    ///   answer. It is the retryable case, and it is the common one.
    /// - `NOT_FOUND` never comes from the catalog service — absence travels as
    ///   an empty field in a successful response. Reaching one means something
    ///   between here and there produced it, which says nothing about whether
    ///   the write would land next time.
    /// - `UNIMPLEMENTED` is a controller too old for this RPC. A rollout fixes
    ///   it, so abandoning the write over one would turn a version skew of a
    ///   few minutes into a permanent disagreement. It stays retryable, and
    ///   the attempt cap in [`super::super::mirror`] is what bounds it.
    fn will_answer_the_same_way(code: tonic::Code) -> bool {
        matches!(
            code,
            tonic::Code::InvalidArgument
                | tonic::Code::FailedPrecondition
                | tonic::Code::OutOfRange
        )
    }

    /// A well-formed response that does not say what it must.
    fn malformed(operation: &'static str, reason: &'static str) -> RepositoryError {
        RepositoryError::backend(
            format!("snapshot catalog '{operation}' answered off contract"),
            anyhow!("{reason}"),
        )
    }

    // ─────────────────────────────────────────────────────────────────────
    // Writes
    // ─────────────────────────────────────────────────────────────────────

    /// Opens a row before any bytes exist.
    ///
    /// `published` and `origin_node_id` are a block and are decided together:
    /// a pause opens unpublished and names this node, because the bytes are
    /// going here and nowhere else yet; a template opens published with no
    /// origin, because nothing is on any node.
    pub async fn begin_snapshot(
        &self,
        record: &SnapshotRecord,
        status: &str,
        published: bool,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        let origin_node_id = if published {
            String::new()
        } else {
            self.node_id.clone()
        };

        let response = self
            .client()
            .begin_snapshot(
                self.request(pb::BeginSnapshotRequest {
                    cluster_id: self.cluster_id.to_string(),
                    node_id: self.node_id.clone(),
                    snapshot_id: record.id.to_string(),
                    source_kind: record_source_kind(record).to_string(),
                    source_sandbox_id: source_sandbox_id(record),
                    cpu_count: record.resources.cpu_count,
                    memory_mib: record.resources.memory_mib,
                    disk_size_mib: record.resources.disk_size_mib,
                    alias: record
                        .alias
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    created_at_unix_ms: record.created_at_unix_ms,
                    sandbox_started_at_unix_ms: None,
                    publishing_execution_id: String::new(),
                    // 🔴 Absent, in this batch, always. Filling it in would make
                    // this RPC write `paused_sandboxes` as well — and the pause
                    // path still drives that table through `PausedRegistry`, so
                    // both halves would be writing the same row from two calls.
                    // The field is the whole point of the service and the batch
                    // that rewires the pause path is the one that sets it.
                    paused_transition: None,
                    status: status.to_string(),
                    published,
                    origin_node_id,
                }),
            )
            .await
            .map_err(|status| Self::unreachable("begin_snapshot", status))?
            .into_inner();

        match response.outcome {
            Some(pb::begin_snapshot_response::Outcome::Began(began)) => {
                let row = began.row.ok_or_else(|| {
                    Self::malformed("begin_snapshot", "an opened row with no row")
                })?;
                Ok(CatalogWrite::Applied(decode_row(row, self.cluster_id)?))
            }
            Some(pb::begin_snapshot_response::Outcome::Rejected(rejected)) => {
                Ok(CatalogWrite::Refused(CatalogRefusal::from_proto(rejected)))
            }
            None => Err(Self::malformed(
                "begin_snapshot",
                "neither an opened row nor a refusal",
            )),
        }
    }

    /// Flips a row to `ready`. The only call that does.
    pub async fn commit_snapshot(
        &self,
        commit: &SnapshotCommit,
        published: bool,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        let origin_node_id = if published {
            String::new()
        } else {
            self.node_id.clone()
        };

        let response = self
            .client()
            .commit_snapshot(
                self.request(pb::CommitSnapshotRequest {
                    cluster_id: self.cluster_id.to_string(),
                    node_id: self.node_id.clone(),
                    snapshot_id: commit.id.to_string(),
                    committed_payload: encode_committed(&commit.committed)?,
                    committed_schema: COMMITTED_PAYLOAD_SCHEMA,
                    cpu_count: Some(commit.resources.cpu_count),
                    memory_mib: Some(commit.resources.memory_mib),
                    disk_size_mib: Some(commit.resources.disk_size_mib),
                    alias: commit
                        .alias
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    updated_at_unix_ms,
                    // See begin_snapshot.
                    paused_transition: None,
                    publishing_execution_id: String::new(),
                    published,
                    origin_node_id,
                }),
            )
            .await
            .map_err(|status| Self::unreachable("commit_snapshot", status))?
            .into_inner();

        match response.outcome {
            Some(pb::commit_snapshot_response::Outcome::Committed(row)) => {
                Ok(CatalogWrite::Applied(decode_row(row, self.cluster_id)?))
            }
            Some(pb::commit_snapshot_response::Outcome::Rejected(rejected)) => {
                Ok(CatalogWrite::Refused(CatalogRefusal::from_proto(rejected)))
            }
            None => Err(Self::malformed(
                "commit_snapshot",
                "neither a committed row nor a refusal",
            )),
        }
    }

    /// Moves a row to `error` with a reason.
    pub async fn fail_snapshot(
        &self,
        id: &SnapshotId,
        reason: &TemplateBuildErrorReason,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        let response = self
            .client()
            .fail_snapshot(self.request(pb::FailSnapshotRequest {
                cluster_id: self.cluster_id.to_string(),
                node_id: self.node_id.clone(),
                snapshot_id: id.to_string(),
                build_error_json: encode_build_error(reason)?,
                updated_at_unix_ms,
                paused_transition: None,
                // 🔴 True, and it has to be. This client admits builds now, and
                // a failed build that left its row `in_progress` would hold the
                // template behind `builds_one_active_per_template` until the
                // reaper's TTL expired — a leak turned into an outage by the
                // very index that makes admission exclusive. Harmless for a
                // snapshot with no build in flight, which every pause is: it is
                // one probe of a partial index that matches nothing.
                fail_active_build: true,
            }))
            .await
            .map_err(|status| Self::unreachable("fail_snapshot", status))?
            .into_inner();

        match response.outcome {
            Some(pb::fail_snapshot_response::Outcome::Failed(row)) => {
                Ok(CatalogWrite::Applied(decode_row(row, self.cluster_id)?))
            }
            Some(pb::fail_snapshot_response::Outcome::Rejected(rejected)) => {
                Ok(CatalogWrite::Refused(CatalogRefusal::from_proto(rejected)))
            }
            None => Err(Self::malformed(
                "fail_snapshot",
                "neither a failed row nor a refusal",
            )),
        }
    }

    /// Admits one build: the cluster-wide ceiling, the per-template exclusion
    /// and the `waiting -> building` transition, in one transaction.
    ///
    /// 🔴 The refusals are the answer, not a failure. A template somebody else
    /// is building and a cluster at its ceiling are both things the caller acts
    /// on, and both are exactly what asking is for.
    ///
    /// `build_id` and `template_id` are the same value because the API forces
    /// them equal today. They are sent separately anyway: the table keeps them
    /// in different columns precisely so that stops being true without a
    /// migration.
    ///
    /// 🔴 The `node_id` this admits the row under is `self.node_id` — see the
    /// field doc. It never carries a node placement decision, because none
    /// has been made yet: this is called from
    /// `v2_templates_template_id_builds_build_id_post` before
    /// `run_the_build_on_a_node` ever asks a `NodePlacement` for a node.
    pub async fn start_build(
        &self,
        id: &SnapshotId,
        build_id: &SnapshotId,
        started_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<StartedBuild>> {
        let response = self
            .client()
            .start_build(self.request(pb::StartBuildRequest {
                cluster_id: self.cluster_id.to_string(),
                node_id: self.node_id.clone(),
                build_id: build_id.to_string(),
                template_id: id.to_string(),
                started_at_unix_ms,
                // 🔴 Advisory. The server stamps the first heartbeat from the
                // database's clock and ignores this, because the reaper judges
                // that value against a clock too and the two have to be the
                // same one — a node running a few minutes slow otherwise had
                // every build it ran ended while it was still running, with an
                // error blaming a lapse that never happened. What the field is
                // still for is telling an operator the clocks disagree, so it
                // is sent honestly rather than left at zero.
                heartbeat_at_unix_ms: started_at_unix_ms,
            }))
            .await
            .map_err(|status| Self::unreachable("start_build", status))?
            .into_inner();

        match response.outcome {
            Some(pb::start_build_response::Outcome::Started(started)) => {
                let row = started.snapshot.ok_or_else(|| {
                    Self::malformed("start_build", "an admitted build without its template row")
                })?;
                Ok(CatalogWrite::Applied(StartedBuild {
                    record: decode_row(row, self.cluster_id)?,
                    build_id: build_id.clone(),
                }))
            }
            Some(pb::start_build_response::Outcome::Rejected(rejected)) => {
                Ok(CatalogWrite::Refused(CatalogRefusal::from_proto(rejected)))
            }
            None => Err(Self::malformed(
                "start_build",
                "neither an admitted build nor a refusal",
            )),
        }
    }

    /// Says this node is still running `build_id`.
    ///
    /// 🔴 `false` means the build is no longer the live one — reaped for a
    /// lapsed heartbeat, or finished by somebody else — and the builder must
    /// **stop**, not retry. Nothing else tells it. Without that, the reaper
    /// frees the template, a second build starts, and two builders publish into
    /// the same one.
    pub async fn renew_build_lease(
        &self,
        build_id: &SnapshotId,
        heartbeat_at_unix_ms: i64,
    ) -> RepositoryResult<bool> {
        let response = self
            .client()
            .renew_build_lease(self.request(pb::RenewBuildLeaseRequest {
                cluster_id: self.cluster_id.to_string(),
                node_id: self.node_id.clone(),
                build_id: build_id.to_string(),
                // Advisory, as on `start_build`.
                heartbeat_at_unix_ms,
            }))
            .await
            .map_err(|status| Self::unreachable("renew_build_lease", status))?
            .into_inner();

        Ok(response.live)
    }

    /// Whether `build_id` is still on the queue.
    ///
    /// `None` when the catalog has no build row for it at all. `Some(true)`
    /// means `pending` or `in_progress`: it is counted against the cluster
    /// ceiling and holds its template behind the per-template exclusion.
    ///
    /// 🔴 Read at the one scope §5.3 names as having to be *without* the
    /// resolvable predicate. This is the reading that exists to look at builds
    /// that are still running or have failed; a `ready`-only reading of it
    /// would answer "no such build" for every build worth asking about.
    pub async fn build_is_active(&self, build_id: &SnapshotId) -> RepositoryResult<Option<bool>> {
        let response = self
            .client()
            .get_build(self.request(pb::GetBuildRequest {
                cluster_id: self.cluster_id.to_string(),
                build_id: build_id.to_string(),
            }))
            .await
            .map_err(|status| Self::unreachable("get_build", status))?
            .into_inner();

        Ok(response
            .build
            .map(|build| matches!(build.status_group.as_str(), "pending" | "in_progress")))
    }

    /// Soft-deletes one row. Idempotent: nothing to delete is a success.
    pub async fn delete_snapshot(
        &self,
        id_or_alias: &str,
        deleted_at_unix_ms: i64,
    ) -> RepositoryResult<bool> {
        let response = self
            .client()
            .delete_snapshot(self.request(pb::DeleteSnapshotRequest {
                cluster_id: self.cluster_id.to_string(),
                id_or_alias: id_or_alias.to_string(),
                deleted_at_unix_ms,
            }))
            .await
            .map_err(|status| Self::unreachable("delete_snapshot", status))?
            .into_inner();

        Ok(response.deleted)
    }

    // ─────────────────────────────────────────────────────────────────────
    // Reads
    // ─────────────────────────────────────────────────────────────────────

    /// Reads one row by id or alias, at an explicitly chosen scope.
    pub async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let response = self
            .client()
            .get_snapshot(self.request(pb::GetSnapshotRequest {
                cluster_id: self.cluster_id.to_string(),
                id_or_alias: id_or_alias.to_string(),
                allow_any_status: scope.allow_any_status(),
                with_build: true,
            }))
            .await
            .map_err(|status| Self::unreachable("get_snapshot", status))?
            .into_inner();

        response
            .row
            .map(|row| decode_row(row, self.cluster_id))
            .transpose()
    }

    /// Resolves an alias, at an explicitly chosen scope.
    pub async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        let response = self
            .client()
            .resolve_alias(self.request(pb::ResolveAliasRequest {
                cluster_id: self.cluster_id.to_string(),
                alias: alias.to_string(),
                allow_any_status: scope.allow_any_status(),
            }))
            .await
            .map_err(|status| Self::unreachable("resolve_alias", status))?
            .into_inner();

        if response.snapshot_id.is_empty() {
            return Ok(None);
        }
        SnapshotId::parse(&response.snapshot_id)
            .map(Some)
            .map_err(|_| Self::malformed("resolve_alias", "an alias target that is not a uuid"))
    }

    /// Reads one keyset page, with the page bounds carried by the filter.
    ///
    /// 🔴 This is the pushdown, and the cost it removes is the whole point of
    /// the batch: an object-store catalog answers `?limit=5` by listing every
    /// object and reading every one of them, so the page size buys nothing.
    /// Here the `limit` and the cursor reach the `WHERE` and the `LIMIT`, and a
    /// small page is a small query.
    pub async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        // 🔴 A caller asking for no rows is answered without a round trip: on
        // the wire `limit = 0` means "the server's default", so forwarding it
        // would turn a request for nothing into a request for a hundred rows.
        if filter.effective_limit() == 0 {
            return Ok(SnapshotListPage::single(Vec::new()));
        }
        self.list_one_page(
            &encode_filter(&filter),
            filter.cursor.as_ref().map(encode_cursor),
            filter.effective_limit(),
            scope,
        )
        .await
    }

    async fn list_one_page(
        &self,
        filter: &pb::SnapshotFilter,
        cursor: Option<pb::SnapshotCursor>,
        limit: u32,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        let response = self
            .client()
            .list_snapshots(self.request(pb::ListSnapshotsRequest {
                cluster_id: self.cluster_id.to_string(),
                filter: Some(filter.clone()),
                cursor,
                limit,
                allow_any_status: scope.allow_any_status(),
                with_build: true,
            }))
            .await
            .map_err(|status| Self::unreachable("list_snapshots", status))?
            .into_inner();

        let mut items = Vec::with_capacity(response.rows.len());
        for row in response.rows {
            items.push(decode_row(row, self.cluster_id)?);
        }
        let next = response
            .next_cursor
            .map(|next| {
                decode_cursor(&next).ok_or_else(|| {
                    Self::malformed("list_snapshots", "a next cursor whose id is not a uuid")
                })
            })
            .transpose()?;

        Ok(SnapshotListPage { items, next })
    }

    /// Walks every page of a listing.
    ///
    /// 🔴 The unbounded read, and it stays unbounded: its callers are the
    /// mirror's history backfill and the comparison that guards the read-side
    /// switch, both of which are counting *everything* and would read a
    /// truncated answer as agreement. Anything answering a user request calls
    /// [`Self::list_page_scoped`] instead.
    pub async fn list_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        let filter = encode_filter(&filter.without_pagination());
        let mut cursor: Option<pb::SnapshotCursor> = None;
        let mut records = Vec::new();

        for _ in 0..LIST_PAGE_LIMIT {
            let page = self
                .list_one_page(&filter, cursor.clone(), LIST_PAGE_SIZE, scope)
                .await?;
            records.extend(page.items);

            match page.next.as_ref().map(encode_cursor) {
                None => return Ok(records),
                Some(next) => {
                    // A cursor that did not move would walk the same page for
                    // as long as the loop bound allows. Refuse it as a protocol
                    // failure rather than returning what has been collected so
                    // far, which would be a short answer nobody could tell from
                    // a complete one.
                    if cursor.as_ref() == Some(&next) {
                        return Err(Self::malformed(
                            "list_snapshots",
                            "a next cursor identical to the one it was given",
                        ));
                    }
                    cursor = Some(next);
                }
            }
        }

        Err(Self::malformed(
            "list_snapshots",
            "a listing that did not end within the page bound",
        ))
    }
}

/// 🔴 The id travels as text, and the server compares it as text.
///
/// The public token orders by the id's string form, and a UUID's binary order
/// is not its text order in general — comparing the wrong one drops rows at a
/// page boundary with no error anywhere. Rendering it here and comparing it
/// there is what keeps the two ends comparing the same thing.
fn encode_cursor(cursor: &SnapshotCursor) -> pb::SnapshotCursor {
    pb::SnapshotCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        snapshot_id: cursor.snapshot_id.to_string(),
    }
}

fn decode_cursor(cursor: &pb::SnapshotCursor) -> Option<SnapshotCursor> {
    SnapshotId::parse(&cursor.snapshot_id)
        .ok()
        .map(|id| SnapshotCursor::new(cursor.created_at_unix_ms, id))
}

fn encode_filter(filter: &SnapshotListFilter) -> pb::SnapshotFilter {
    pb::SnapshotFilter {
        source_kinds: filter
            .sources
            .as_ref()
            .map(|kinds| {
                kinds
                    .iter()
                    .map(|kind| source_kind_str(*kind).to_string())
                    .collect()
            })
            .unwrap_or_default(),
        alias_prefix: filter.alias_prefix.clone(),
        snapshot_ids: filter
            .snapshot_ids
            .as_ref()
            .map(|ids| ids.iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        snapshot_id_or_alias: filter.snapshot_id_or_alias.clone(),
        source_sandbox_id: filter.source_sandbox_id.clone(),
        template_statuses: filter
            .template_statuses
            .as_ref()
            .map(|statuses| {
                statuses
                    .iter()
                    .map(|status| build_status_str(*status).to_string())
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// How a refused build admission reaches the user.
///
/// 🔴 `InvalidRequest`, which the API layer renders as a 400, because every one
/// of these is something the caller can do something about: wait for the build
/// that holds the template, wait for the cluster to drain, or look at why the
/// row is not waiting. A `Backend` error would render as a 500 and tell them to
/// retry something that will be refused identically.
fn build_refusal(id: &SnapshotId, refusal: CatalogRefusal) -> RepositoryError {
    match refusal {
        CatalogRefusal::NotFound => RepositoryError::SnapshotNotFound {
            lookup: id.to_string(),
        },
        other => RepositoryError::InvalidRequest {
            reason: format!("build '{id}' was not admitted: {other}"),
        },
    }
}

fn refused(operation: &'static str, refusal: CatalogRefusal) -> RepositoryError {
    RepositoryError::backend(
        format!("snapshot catalog refused '{operation}'"),
        anyhow!("{refusal}"),
    )
}

#[async_trait]
impl SnapshotCatalog for CentralSnapshotCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        match self
            .begin_snapshot(&record, opening_status(&record), true)
            .await?
        {
            CatalogWrite::Applied(row) => Ok(row),
            CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
                Err(alias_conflict(record.alias.as_ref(), &record.id, holder))
            }
            CatalogWrite::Refused(refusal) => Err(refused("create", refusal)),
        }
    }

    /// Opens the row if nobody has, then flips it.
    ///
    /// 🔴 Two statements, matching the two the design calls A and B. The first
    /// is what makes a pause's row exist at all — a captured sandbox has no
    /// catalog row until it publishes — and `ALREADY_EXISTS` from it is the
    /// ordinary answer on the template path, where the row was created when the
    /// template was.
    ///
    /// The fence lives in the second: the server only flips a row that is
    /// `building`, and refuses one that is not. That is what leaves a crash
    /// between the bytes and this call harmless — the row stays `in_progress`,
    /// and every resolving query already refuses to see it.
    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        let opening = commit_opening_record(&commit);
        // Unpublished, naming this node: the bytes are here and, until the
        // commit says otherwise, nowhere else.
        match self
            .begin_snapshot(&opening, STATUS_BUILDING, false)
            .await?
        {
            CatalogWrite::Applied(_) | CatalogWrite::Refused(CatalogRefusal::AlreadyExists) => {}
            CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
                return Err(alias_conflict(commit.alias.as_ref(), &commit.id, holder))
            }
            CatalogWrite::Refused(refusal) => return Err(refused("publish_commit", refusal)),
        }

        match self.commit_snapshot(&commit, true, now_unix_ms()).await? {
            CatalogWrite::Applied(row) => Ok(row),
            CatalogWrite::Refused(CatalogRefusal::AliasTaken { holder }) => {
                Err(alias_conflict(commit.alias.as_ref(), &commit.id, holder))
            }
            CatalogWrite::Refused(refusal) => Err(refused("publish_commit", refusal)),
        }
    }

    /// 🔴 Resolvable rows only, and that is the safe reading rather than the
    /// convenient one.
    ///
    /// This is the read a resume reaches, and a `building` row it could see is
    /// a snapshot whose bytes are still uploading. The endpoint that must see
    /// those — the one reporting build status — asks for them by name through
    /// [`CentralSnapshotCatalog::get_scoped`]; a caller that says nothing gets
    /// the reading that cannot start a half-written VM.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        CentralSnapshotCatalog::get_scoped(self, id_or_alias, CatalogReadScope::Resolvable).await
    }

    /// 🔴 The override that makes the scope mean anything.
    ///
    /// This is the one catalog in the tree whose storage can hide a row by
    /// status, so it is the one whose reads change with the scope. Delegating
    /// to the trait's default here — or forgetting the override on a future
    /// backend that also has a status column — is exactly the shape of the
    /// defect: every template surface would go on asking for `AnyStatus` and
    /// go on being answered `ready`.
    async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        CentralSnapshotCatalog::get_scoped(self, id_or_alias, scope).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.list_scoped(filter, CatalogReadScope::Resolvable).await
    }

    /// See [`Self::get`] on why this is the resolvable reading.
    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        CentralSnapshotCatalog::list_page_scoped(self, filter, CatalogReadScope::Resolvable).await
    }

    /// See [`SnapshotCatalog::get_scoped`] on this catalog.
    async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        CentralSnapshotCatalog::list_page_scoped(self, filter, scope).await
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        // The alias goes with the row: `aliases.snapshot_id` is a foreign key
        // with ON DELETE CASCADE and the soft delete drops the binding in the
        // same statement, so there is nothing here to unbind separately.
        let deleted = self
            .delete_snapshot(&record.id.to_string(), now_unix_ms())
            .await?;
        if !deleted {
            debug!(snapshot_id = %record.id, "snapshot catalog had nothing to delete");
        }
        Ok(())
    }

    /// See [`Self::get`] on why this is the resolvable reading.
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        CentralSnapshotCatalog::resolve_alias_scoped(self, alias, CatalogReadScope::Resolvable)
            .await
    }

    /// See [`SnapshotCatalog::get_scoped`] on this catalog.
    async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        CentralSnapshotCatalog::resolve_alias_scoped(self, alias, scope).await
    }

    /// The `waiting -> building` transition, which here is build admission:
    /// the cluster-wide ceiling, the per-template unique index, and a `builds`
    /// row only a heartbeat and a reaper release.
    ///
    /// 🔴 A refusal is reported to the caller rather than swallowed. Admitting
    /// a build the catalog said no to is the "two builders publishing into one
    /// template" the exclusion exists to prevent, and a caller told its build
    /// started when it did not would wait for a result nobody is producing.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        // 🔴 A new id every time, never the template's. The catalog keys a
        // build row by the build id, so reusing the template's made the second
        // admission collide with the first row that ever existed — which is
        // what retrying a failed build is. The table has kept the two ids in
        // separate columns from the start for exactly this.
        match self
            .start_build(id, &SnapshotId::generate(), now_unix_ms())
            .await?
        {
            CatalogWrite::Applied(started) => Ok(started),
            CatalogWrite::Refused(refusal) => Err(build_refusal(id, refusal)),
        }
    }

    /// See [`Self::renew_build_lease`]. `false` means stop.
    async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        CentralSnapshotCatalog::renew_build_lease(self, build_id, now_unix_ms()).await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        match self.fail_snapshot(id, &reason, now_unix_ms()).await? {
            CatalogWrite::Applied(_) => Ok(()),
            CatalogWrite::Refused(refusal) => Err(refused("mark_build_error", refusal)),
        }
    }
}

/// The record `publish_commit` opens its row from.
///
/// A commit describes a snapshot, not a row, so the opening row is derived: a
/// sandbox commit opens a sandbox row naming its sandbox, a template commit
/// opens a template row. The alias is deliberately *not* carried here — the
/// commit binds it, in the same transaction as the flip, and binding it twice
/// would make a rename look like a collision.
pub fn commit_opening_record(commit: &SnapshotCommit) -> SnapshotRecord {
    let source = match &commit.source {
        crate::snapshot::types::SnapshotPublishSource::Sandbox { source_sandbox_id } => {
            SnapshotSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            }
        }
        crate::snapshot::types::SnapshotPublishSource::Template => SnapshotSource::Template {
            build: crate::snapshot::types::TemplateBuildInfo::waiting(),
        },
    };

    // 🔴 The commit's instant, not this call's. A replay runs hours or days
    // after the snapshot was made, and stamping the replay's clock here is what
    // rewrote every backfilled row's creation time to the moment the backfill
    // ran — the column the listing orders by. `updated_at` is the replay's
    // clock on purpose: a replay really is the last thing that touched the row.
    let now = now_unix_ms();
    SnapshotRecord {
        id: commit.id.clone(),
        alias: None,
        source,
        resources: commit.resources,
        created_at_unix_ms: commit.created_at_unix_ms.unwrap_or(now),
        updated_at_unix_ms: now,
        committed: None,
    }
}

/// 🔴 `alias` comes from the write that was refused, not from whatever record
/// happened to be in hand. `publish_commit` opens its row from a *derived*
/// record that deliberately carries no alias — the commit binds it — so passing
/// that record here reported an empty name to a user whose publish was refused
/// over a name they had asked for.
pub fn alias_conflict(
    alias: Option<&SnapshotAlias>,
    id: &SnapshotId,
    holder: String,
) -> RepositoryError {
    match SnapshotId::parse(&holder) {
        Ok(existing) => RepositoryError::AliasConflict {
            alias: alias.map(ToString::to_string).unwrap_or_default(),
            existing,
            new_id: id.clone(),
        },
        // The holder is only reported so the error can name it. A holder that
        // did not parse still means the name is taken, and reporting that as a
        // decode failure would turn a refusal a caller can act on into one it
        // cannot.
        Err(_) => RepositoryError::backend(
            "snapshot catalog refused an alias binding",
            anyhow!("the alias is held by '{holder}'"),
        ),
    }
}

/// Whether a failure this client produced will come back the same next time.
///
/// 🔴 The seam between [`CentralSnapshotCatalog::will_answer_the_same_way`] and
/// everything that has to act on the distinction. Stated as a function over the
/// error rather than left as a `matches!` at each call site, because there are
/// three of them — the forward write, the repair pass's verdict, and the tests
/// that hold both still — and three copies of one rule is three places for it
/// to drift.
///
/// Scoped to errors that came *out of this module*. A caller applying it to an
/// error from somewhere else is asking a question it does not answer.
pub fn is_permanent_failure(error: &RepositoryError) -> bool {
    matches!(error, RepositoryError::InvalidRequest { .. })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 The inversion, in the one place it happens.
    ///
    /// The wire field is `allow_any_status` so that its zero value — what a
    /// caller that never heard of it sends — keeps the predicate that stops a
    /// half-uploaded snapshot from starting a VM. This side keeps the positive
    /// reading, and this is the seam between them. A `Resolvable` scope that
    /// sent `true` would resolve exactly the rows the field exists to hide.
    #[test]
    fn the_resolvable_scope_never_asks_for_any_status() {
        assert!(!CatalogReadScope::Resolvable.allow_any_status());
        assert!(CatalogReadScope::AnyStatus.allow_any_status());
    }

    /// The row `publish_commit` opens carries no alias: the commit binds it, in
    /// the same transaction as the flip. Binding it twice would make a rename
    /// look like a collision with itself.
    #[test]
    fn the_opening_row_leaves_the_alias_to_the_commit() {
        let commit = SnapshotCommit {
            id: SnapshotId::generate(),
            alias: Some(
                crate::snapshot::types::SnapshotAlias::parse("named").expect("alias parses"),
            ),
            source: crate::snapshot::types::SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx".to_string(),
            },
            resources: crate::types::SandboxResources::default(),
            created_at_unix_ms: None,
            committed: crate::snapshot::types::CommittedSnapshot::mock(),
        };

        let opening = commit_opening_record(&commit);
        assert!(opening.alias.is_none());
        assert_eq!(opening.id, commit.id);
        assert!(matches!(
            opening.source,
            SnapshotSource::Sandbox { ref source_sandbox_id } if source_sandbox_id == "sbx"
        ));
        assert_eq!(opening_status(&opening), STATUS_BUILDING);
    }

    /// 🔴 The alias a refused publish reports is the one the caller asked for.
    /// It used to be read off the derived opening record, which deliberately
    /// has none, so a user whose publish lost a name was told the empty name
    /// had collided.
    #[test]
    fn a_refused_alias_is_reported_by_the_name_the_caller_asked_for() {
        let holder = SnapshotId::generate();
        let mine = SnapshotId::generate();
        let alias = crate::snapshot::types::SnapshotAlias::parse("contested").expect("parses");

        match alias_conflict(Some(&alias), &mine, holder.to_string()) {
            RepositoryError::AliasConflict {
                alias: reported,
                existing,
                new_id,
            } => {
                assert_eq!(reported, "contested");
                assert_eq!(existing, holder);
                assert_eq!(new_id, mine);
            }
            other => panic!("expected an alias conflict, got {other:?}"),
        }
    }

    /// A holder id that will not parse still means the name is taken. Reporting
    /// it as a decode failure would turn a refusal the caller can act on into
    /// one it cannot.
    #[test]
    fn an_unreadable_holder_still_reports_the_name_as_taken() {
        let error = alias_conflict(None, &SnapshotId::generate(), "not-a-uuid".to_string());
        assert!(format!("{error}").contains("refused an alias binding"));
    }

    /// 🔴 A status the controller will produce again is not an outage. Reading
    /// it as one is what made every template create's rejection into debt that
    /// was replayed every thirty seconds forever while the lag sat frozen — and
    /// the lag is what the read-side switch is gated on.
    #[test]
    fn a_status_the_controller_will_repeat_is_not_reported_as_unreachable() {
        for status in [
            tonic::Status::invalid_argument("disk_size_mib must be positive"),
            tonic::Status::failed_precondition("this row predates the check"),
            tonic::Status::out_of_range("that page does not exist"),
        ] {
            let code = status.code();
            let error = CentralSnapshotCatalog::unreachable("begin_snapshot", status);
            assert!(
                is_permanent_failure(&error),
                "{code} is the controller stating a rule, not a transport fault: {error}"
            );
        }
    }

    /// The control face, and the one that matters more: everything else stays
    /// retryable. Reading a restarting scheduler as a permanent rejection would
    /// abandon writes the compensator would have landed a minute later.
    #[test]
    fn everything_else_is_still_reported_as_unreachable() {
        for status in [
            tonic::Status::unavailable("the scheduler is rolling"),
            tonic::Status::deadline_exceeded("ten seconds"),
            tonic::Status::unimplemented("this controller is older than this node"),
            tonic::Status::not_found("something between here and there produced this"),
            tonic::Status::internal("the controller fell over"),
        ] {
            let code = status.code();
            let error = CentralSnapshotCatalog::unreachable("commit_snapshot", status);
            assert!(
                !is_permanent_failure(&error),
                "{code} says nothing about whether the write would land next time: {error}"
            );
            assert!(matches!(error, RepositoryError::Backend { .. }));
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // `builds.node_id`: who StartBuild and RenewBuildLease say this is
    // ─────────────────────────────────────────────────────────────────────
    //
    // A fake `SnapshotCatalog` gRPC service, real socket and all — matching
    // `src/node_client/tests.rs`'s harness for the node service next door —
    // because the fact under test lives entirely in what goes out on the
    // wire. Nothing above this file ever supplies a node id to `start_build`
    // or `renew_build_lease`: both read `self.node_id`, so a fake that only
    // inspected the `SnapshotCatalog` (repository) trait's arguments could
    // never observe the bug this guards — that trait does not carry a
    // `node_id` parameter at all.
    mod node_id_semantics {
        use std::sync::{Arc, Mutex};

        use tokio::sync::oneshot;
        use tonic::{Request, Response, Status};
        use uuid::Uuid;

        use super::super::CentralSnapshotCatalog;
        use crate::proto::scheduler as pb;
        use crate::snapshot::types::SnapshotId;

        /// Answers `StartBuild` and `RenewBuildLease` by recording the
        /// `node_id` each request carried, and refuses everything else —
        /// this harness exists to answer exactly one question.
        #[derive(Default)]
        struct RecordingCatalog {
            start_build_node_id: Mutex<Option<String>>,
            renew_build_lease_node_id: Mutex<Option<String>>,
        }

        #[tonic::async_trait]
        impl pb::snapshot_catalog_server::SnapshotCatalog for RecordingCatalog {
            async fn begin_snapshot(
                &self,
                _request: Request<pb::BeginSnapshotRequest>,
            ) -> Result<Response<pb::BeginSnapshotResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn commit_snapshot(
                &self,
                _request: Request<pb::CommitSnapshotRequest>,
            ) -> Result<Response<pb::CommitSnapshotResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn fail_snapshot(
                &self,
                _request: Request<pb::FailSnapshotRequest>,
            ) -> Result<Response<pb::FailSnapshotResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn get_snapshot(
                &self,
                _request: Request<pb::GetSnapshotRequest>,
            ) -> Result<Response<pb::GetSnapshotResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn list_snapshots(
                &self,
                _request: Request<pb::ListSnapshotsRequest>,
            ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn delete_snapshot(
                &self,
                _request: Request<pb::DeleteSnapshotRequest>,
            ) -> Result<Response<pb::DeleteSnapshotResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn resolve_alias(
                &self,
                _request: Request<pb::ResolveAliasRequest>,
            ) -> Result<Response<pb::ResolveAliasResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
            async fn start_build(
                &self,
                request: Request<pb::StartBuildRequest>,
            ) -> Result<Response<pb::StartBuildResponse>, Status> {
                *self.start_build_node_id.lock().expect("lock") =
                    Some(request.into_inner().node_id);
                Err(Status::unimplemented(
                    "this harness only records what StartBuild carried",
                ))
            }
            async fn renew_build_lease(
                &self,
                request: Request<pb::RenewBuildLeaseRequest>,
            ) -> Result<Response<pb::RenewBuildLeaseResponse>, Status> {
                *self.renew_build_lease_node_id.lock().expect("lock") =
                    Some(request.into_inner().node_id);
                Err(Status::unimplemented(
                    "this harness only records what RenewBuildLease carried",
                ))
            }
            async fn get_build(
                &self,
                _request: Request<pb::GetBuildRequest>,
            ) -> Result<Response<pb::GetBuildResponse>, Status> {
                Err(Status::unimplemented("not exercised by this test"))
            }
        }

        async fn serve(catalog: Arc<RecordingCatalog>) -> (String, oneshot::Sender<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a port");
            let addr = listener.local_addr().expect("the bound address");
            let (tx, rx) = oneshot::channel();

            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(
                        pb::snapshot_catalog_server::SnapshotCatalogServer::from_arc(catalog),
                    )
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
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            (format!("http://{addr}"), tx)
        }

        /// 🔴 The regression this guards: `builds.node_id` must keep naming
        /// the process administering the build's lease — here,
        /// `"agentenv-api-6968b44f7c-4j5bn"`, standing in for an API
        /// replica — and never a node a placement decision chose to run the
        /// build sandbox on, standing in here as `"aenv-worker-01"`. This
        /// catalog client is never even told about the second value: nothing
        /// downstream of `CentralSnapshotCatalog::connect_lazy` can make it
        /// leak in, so a `start_build` or `renew_build_lease` implementation
        /// that sent anything other than `self.node_id` — a hardcoded
        /// string, an empty one, or (the change problem one specifically
        /// warns against) a future `executor_node_id` parameter plumbed in
        /// from placement — turns one or both assertions below red.
        ///
        /// Both calls are asserted, and asserted equal to each other, because
        /// that equality is precisely what lets `renewBuildLeaseSQL`'s
        /// `WHERE node_id = $2` find the row `start_build` opened: if a fix
        /// for "this column should be the executor" changed only one of the
        /// two call sites, the row `start_build` admits would never renew
        /// again and the reaper would end a build that is still running.
        #[tokio::test]
        async fn start_build_and_its_heartbeats_name_the_lease_holder_not_a_placement_choice() {
            let catalog = Arc::new(RecordingCatalog::default());
            let (endpoint, _shutdown) = serve(Arc::clone(&catalog)).await;

            // Never "aenv-worker-01": that name stands for a node a
            // `NodePlacement` might choose later, which this client is never
            // told about.
            let this_replica = "agentenv-api-6968b44f7c-4j5bn".to_string();
            let client = CentralSnapshotCatalog::connect_lazy(
                &endpoint,
                Uuid::new_v4(),
                this_replica.clone(),
            )
            .expect("a lazily-connected client");

            let build_id = SnapshotId::generate();
            let _ = client
                .start_build(&build_id, &build_id, 1_700_000_000_000)
                .await;
            let _ = client.renew_build_lease(&build_id, 1_700_000_001_000).await;

            assert_eq!(
                catalog.start_build_node_id.lock().expect("lock").as_deref(),
                Some(this_replica.as_str()),
                "StartBuild must name the replica admitting the build, not a node any placement \
                 decision picked — this client has no such node to send"
            );
            assert_eq!(
                catalog
                    .renew_build_lease_node_id
                    .lock()
                    .expect("lock")
                    .as_deref(),
                Some(this_replica.as_str()),
                "RenewBuildLease must keep sending the exact identity StartBuild admitted the row \
                 under, or the reaper's `node_id = $2` predicate stops matching a heartbeat this \
                 build is still sending"
            );
        }
    }
}
