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
use tonic::transport::{Channel, Endpoint};
use tracing::debug;
use uuid::Uuid;

use crate::proto::scheduler as pb;
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
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

/// Whether a read may see rows that are not resolvable yet.
///
/// 🔴 There is deliberately no `Default`, and the enum is deliberately not a
/// `bool`. `true` is what stops a snapshot whose bytes are still uploading from
/// starting a VM; `false` is what lets the build-status endpoint see a build
/// that is running or has failed. Neither is safe to guess, so every call site
/// says which it wants, out loud, and a new one cannot compile without
/// deciding.
///
/// The wire field is spelled the other way round — `allow_any_status`, whose
/// zero value is the safe reading — because proto3 cannot make a bool required.
/// The inversion happens in exactly one place, [`Self::allow_any_status`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogReadScope {
    /// Only rows a caller may launch from: `status_group = 'ready'`.
    Resolvable,
    /// Every row, including `waiting`, `building` and `error`.
    ///
    /// The build-status endpoint is what this exists for. Nothing that resolves
    /// a snapshot in order to run it may ask for it.
    AnyStatus,
}

impl CatalogReadScope {
    fn allow_any_status(self) -> bool {
        matches!(self, Self::AnyStatus)
    }
}

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
    channel: Channel,
    cluster_id: Uuid,
    /// This machine. It is the `node_id` every write carries, and — for a
    /// snapshot this node is staging — the `origin_node_id` the row records.
    node_id: String,
    call_timeout: Duration,
}

impl CentralSnapshotCatalog {
    /// Builds a client pointed at `endpoint`.
    ///
    /// Lazy, like every other gRPC client here: the endpoint is parsed now and
    /// dialled on first use, so a controller that is still rolling does not
    /// stop the node from starting.
    pub fn connect_lazy(endpoint: &str, cluster_id: Uuid, node_id: String) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|error| anyhow!("invalid scheduler endpoint '{endpoint}': {error}"))?
            .connect_lazy();

        Ok(Self::over_channel(channel, cluster_id, node_id))
    }

    pub fn over_channel(channel: Channel, cluster_id: Uuid, node_id: String) -> Self {
        Self {
            channel,
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
        pb::snapshot_catalog_client::SnapshotCatalogClient::new(self.channel.clone())
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
                // 🔴 False, in this batch. Ending a build row is only correct
                // for a build this client admitted, and it admits none: build
                // admission is not wired here. See `try_start_build`.
                fail_active_build: false,
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

    /// Walks every page of a listing.
    ///
    /// 🔴 Keyset pagination is pushed down to the server, but the *trait* still
    /// returns every matching row, so this drains the cursor rather than
    /// answering with one page. Handing back a single page here would silently
    /// truncate every caller — and the callers are listing endpoints, so the
    /// truncation would read as "the cluster has this many snapshots".
    /// Surfacing the cursor to the API layer is the next batch's work.
    pub async fn list_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        let filter = encode_filter(&filter);
        let mut cursor: Option<pb::SnapshotCursor> = None;
        let mut records = Vec::new();

        for _ in 0..LIST_PAGE_LIMIT {
            let response = self
                .client()
                .list_snapshots(self.request(pb::ListSnapshotsRequest {
                    cluster_id: self.cluster_id.to_string(),
                    filter: Some(filter.clone()),
                    cursor: cursor.clone(),
                    limit: LIST_PAGE_SIZE,
                    allow_any_status: scope.allow_any_status(),
                    with_build: true,
                }))
                .await
                .map_err(|status| Self::unreachable("list_snapshots", status))?
                .into_inner();

            for row in response.rows {
                records.push(decode_row(row, self.cluster_id)?);
            }

            match response.next_cursor {
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

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
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
        self.get_scoped(id_or_alias, CatalogReadScope::Resolvable)
            .await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.list_scoped(filter, CatalogReadScope::Resolvable).await
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
        self.resolve_alias_scoped(alias, CatalogReadScope::Resolvable)
            .await
    }

    /// 🔴 Not wired in this batch, and refusing is not an option either.
    ///
    /// The catalog's `waiting -> building` transition is `StartBuild`, which is
    /// build admission: the cluster-wide ceiling, the per-template unique index
    /// and a `builds` row that only a heartbeat and a reaper can ever release.
    /// Neither of those is running, so admitting a build here would make the
    /// first crashed builds hold their templates shut forever, and the
    /// twentieth would hold the whole cluster shut. That is precisely the
    /// failure the design warns about when it says the index and the reaper
    /// must ship together.
    ///
    /// So this reports success without writing. What it costs is stated where
    /// it can be seen rather than hidden: a template row in the catalog stays
    /// `waiting` while the object store's has moved on, and the commit that
    /// follows finds a row it cannot flip. The double-write wrapper classifies
    /// that as a mirror divergence and counts it; it does not fail the publish,
    /// because the object store is still the side that answers reads.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        Err(RepositoryError::Unsupported {
            feature: format!(
                "starting build '{id}' through the central catalog: the transition is build \
                 admission, which needs the heartbeat and the reaper that are not wired yet"
            ),
        })
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
pub(crate) fn alias_conflict(
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
pub(crate) fn is_permanent_failure(error: &RepositoryError) -> bool {
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
}
