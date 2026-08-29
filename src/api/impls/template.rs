use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use chrono::TimeZone;
use headers::Host;
use http::Method;
use tracing::{info, warn};

use agentenv_http_server::apis::templates::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::pagination::{snapshot_cursor_from_token, snapshot_next_token};
use super::template_helpers::{
    template_build_record_from_v3_request, template_build_spec_from_start_request,
    template_build_start_base_source, TemplateBuildStartBaseSource,
};
use super::ApiImpl;
use crate::proto::node as pb;
use crate::sandbox::CapturedSandboxSnapshot;
use crate::snapshot::{
    CatalogReadScope, CommandContext, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions,
    SnapshotSource, TemplateBuildErrorReason, TemplateBuildStatus,
};
use crate::types::{ImageConfigs, SandboxResources};
use crate::virtualization::VirtualizationMode;

impl From<TemplateBuildStatus> for models::TemplateBuildStatus {
    fn from(status: TemplateBuildStatus) -> Self {
        match status {
            TemplateBuildStatus::Waiting => Self::Waiting,
            TemplateBuildStatus::Building => Self::Building,
            TemplateBuildStatus::Ready => Self::Ready,
            TemplateBuildStatus::Error => Self::Error,
        }
    }
}

fn datetime_from_unix_ms(unix_ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .timestamp_millis_opt(unix_ms)
        .single()
        .unwrap_or_else(chrono::Utc::now)
}

fn build_record_names(record: &SnapshotRecord) -> Vec<String> {
    record
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_string()])
        .unwrap_or_else(|| vec![record.id.to_string()])
}

fn build_record_envd_version(record: &SnapshotRecord) -> String {
    record
        .committed
        .as_ref()
        .map(|snapshot| snapshot.runtime_versions.envd_version.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn template_build_status(record: &SnapshotRecord) -> TemplateBuildStatus {
    match &record.source {
        SnapshotSource::Template { build } => build.status,
        SnapshotSource::Sandbox { .. } => TemplateBuildStatus::Ready,
    }
}

impl From<SnapshotRecord> for models::Template {
    fn from(record: SnapshotRecord) -> Self {
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
        Self::new(
            record.id.to_string(),
            record.id.to_string(),
            record.resources.cpu_count,
            record.resources.memory_mib,
            record.resources.disk_size_mib,
            true,
            build_record_names(&record),
            created_at,
            updated_at,
            Nullable::Null,
            0,
            1,
            build_record_envd_version(&record),
            template_build_status(&record).into(),
        )
    }
}

impl From<&SnapshotRecord> for models::TemplateBuild {
    fn from(record: &SnapshotRecord) -> Self {
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
        let build_id = record.id.0;
        let finished_at = match &record.source {
            SnapshotSource::Template { build } => {
                build.finished_at_unix_ms.map(datetime_from_unix_ms)
            }
            SnapshotSource::Sandbox { .. } => None,
        };
        Self {
            build_id,
            status: template_build_status(record).into(),
            created_at,
            updated_at,
            finished_at,
            cpu_count: record.resources.cpu_count,
            memory_mb: record.resources.memory_mib,
            disk_size_mb: Some(record.resources.disk_size_mib),
            envd_version: record
                .committed
                .as_ref()
                .map(|snapshot| snapshot.runtime_versions.envd_version.clone()),
        }
    }
}

impl From<SnapshotRecord> for models::TemplateBuildInfo {
    fn from(record: SnapshotRecord) -> Self {
        let mut info = Self::new(
            Vec::new(),
            Vec::new(),
            record.id.to_string(),
            record.id.to_string(),
            template_build_status(&record).into(),
        );
        if let SnapshotSource::Template { build } = &record.source {
            if let Some(error_reason) = &build.error_reason {
                info.reason = Some(models::BuildStatusReason {
                    message: error_reason.message.clone(),
                    step: error_reason.step.clone(),
                    log_entries: None,
                });
            }
        }
        info
    }
}

fn v2_start_build_error(err: models::Error) -> V2TemplatesTemplateIdBuildsBuildIdPostResponse {
    match err.code {
        400 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status400_BadRequest(err),
        404 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(err),
        _ => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status500_ServerError(err),
    }
}

async fn mark_v2_build_error(
    api: &ApiImpl,
    build_id: &SnapshotId,
    reason: TemplateBuildErrorReason,
) {
    if let Err(error) = api
        .snapshot_manager
        .mark_build_error(build_id, reason)
        .await
    {
        warn!(
            build_id = %build_id,
            error = %format_args!("{error:#}"),
            "failed to persist template build error status"
        );
    }
}

#[async_trait]
impl Templates<()> for ApiImpl {
    type Claims = super::Claims;

    async fn templates_aliases_alias_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesAliasesAliasGetPathParams,
    ) -> Result<TemplatesAliasesAliasGetResponse, ()> {
        if let Err(err) = SnapshotAlias::parse(&path_params.alias) {
            return Ok(TemplatesAliasesAliasGetResponse::Status400_BadRequest(
                Self::error(400, err.to_string()),
            ));
        }

        // 🔴 Every row. This is the template surface's name lookup — `aenv`
        // turns every id-or-name argument into a call to it — and a template
        // is `waiting` from creation until its first build commits. Resolving
        // it only when it is `ready` means a template cannot be watched,
        // deleted or built by the name it was created with. Nothing here
        // launches anything: the id it returns still has to pass the
        // resolvable read on the launch path.
        match self
            .snapshot_manager
            .resolve_alias_scoped(&path_params.alias, CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(snapshot_id)) => Ok(
                TemplatesAliasesAliasGetResponse::Status200_SuccessfullyQueriedTemplateByAlias(
                    models::TemplateAliasResponse::new(snapshot_id.to_string(), false),
                ),
            ),
            Ok(None) => Ok(TemplatesAliasesAliasGetResponse::Status404_NotFound(
                Self::error(
                    404,
                    format!("template alias not found: {}", path_params.alias),
                ),
            )),
            Err(err) => Ok(TemplatesAliasesAliasGetResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }

    /// 🔴 Paginated as of this batch, where it never was.
    ///
    /// It is the deprecated listing, and it was also the only one that read the
    /// whole catalog with nothing bounding it — so on a large catalog it is the
    /// first endpoint to fall over, while the acceptance criterion everyone
    /// watches is on `GET /snapshots`. Both parameters are optional and the
    /// header only appears when there is another page, so a client that ignores
    /// all three still gets a valid response; what it no longer gets is every
    /// row.
    async fn templates_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::TemplatesGetQueryParams,
    ) -> Result<TemplatesGetResponse, ()> {
        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match snapshot_cursor_from_token(token) {
                Ok(cursor) => Some(cursor),
                Err(err) => {
                    return Ok(TemplatesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {err}"),
                    )));
                }
            },
            None => None,
        };

        let page = match self
            .snapshot_manager
            .list_page_scoped(
                SnapshotListFilter::templates().paginated(query_params.limit, cursor),
                CatalogReadScope::AnyStatus,
            )
            .await
        {
            Ok(page) => page,
            Err(err) => {
                return Ok(TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        Ok(
            TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates {
                body: page.items.into_iter().map(models::Template::from).collect(),
                x_next_token: page.next.as_ref().map(snapshot_next_token),
            },
        )
    }

    async fn v2_templates_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::V2TemplatesGetQueryParams,
    ) -> Result<V2TemplatesGetResponse, ()> {
        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match snapshot_cursor_from_token(token) {
                Ok(cursor) => Some(cursor),
                Err(err) => {
                    return Ok(V2TemplatesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {err}"),
                    )));
                }
            },
            None => None,
        };

        let page = match self
            .snapshot_manager
            .list_page_scoped(
                SnapshotListFilter::templates().paginated(query_params.limit, cursor),
                CatalogReadScope::AnyStatus,
            )
            .await
        {
            Ok(page) => page,
            Err(err) => {
                return Ok(V2TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        Ok(
            V2TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates {
                body: page.items.into_iter().map(models::Template::from).collect(),
                x_next_token: page.next.as_ref().map(snapshot_next_token),
            },
        )
    }

    async fn templates_template_id_builds_build_id_status_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdBuildsBuildIdStatusGetPathParams,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdStatusGetResponse, ()> {
        if path_params.template_id != path_params.build_id {
            return Ok(
                TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status404_NotFound(Self::error(
                    404,
                    format!(
                        "build {} not found for template {}",
                        path_params.build_id, path_params.template_id
                    ),
                )),
            );
        }

        // 🔴 Every row, and this endpoint above all others. Its whole job is
        // to report `waiting`, `building` and `error` — the three the
        // resolvable reading hides — so at that scope it answered 404 for
        // every build that had not finished, which is every build a caller
        // polls it about.
        match self
            .snapshot_manager
            .get_scoped(&path_params.template_id, CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) => {
                Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status200_SuccessfullyReturnedTheTemplate(
                    record.into(),
                ))
            }
            Ok(None) => Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status404_NotFound(
                Self::error(
                    404,
                    format!("template {} not found", path_params.template_id),
                ),
            )),
            Err(err) => Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }

    async fn templates_template_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdDeletePathParams,
    ) -> Result<TemplatesTemplateIdDeleteResponse, ()> {
        match self.snapshot_manager.delete(&path_params.template_id).await {
            Ok(_) => {
                Ok(TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully)
            }
            Err(err) => Ok(TemplatesTemplateIdDeleteResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }

    async fn templates_template_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdGetPathParams,
        query_params: &models::TemplatesTemplateIdGetQueryParams,
    ) -> Result<TemplatesTemplateIdGetResponse, ()> {
        // 🔴 Every row: the response carries the build's status and error
        // reason, so the states this must show are exactly the ones the
        // resolvable reading refuses to return.
        let record = match self
            .snapshot_manager
            .get_scoped(&path_params.template_id, CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(TemplatesTemplateIdGetResponse::Status404_NotFound(
                    Self::error(
                        404,
                        format!("template {} not found", path_params.template_id),
                    ),
                ));
            }
            Err(err) => {
                return Ok(TemplatesTemplateIdGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let mut builds = if query_params.next_token.is_some() {
            Vec::new()
        } else {
            vec![models::TemplateBuild::from(&record)]
        };
        if let Some(limit) = query_params.limit {
            builds.truncate(limit as usize);
        }
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);

        Ok(
            TemplatesTemplateIdGetResponse::Status200_SuccessfullyReturnedTheTemplateWithItsBuilds(
                models::TemplateWithBuilds::new(
                    record.id.to_string(),
                    true,
                    build_record_names(&record),
                    created_at,
                    updated_at,
                    Nullable::Null,
                    0,
                    builds,
                ),
            ),
        )
    }

    async fn v3_templates_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::TemplateBuildRequestV3,
    ) -> Result<V3TemplatesPostResponse, ()> {
        let Some(name) = body.name.clone() else {
            return Ok(V3TemplatesPostResponse::Status400_BadRequest(Self::error(
                400,
                "template name must be provided",
            )));
        };

        let snapshot_id = SnapshotId::generate();
        let record = match template_build_record_from_v3_request(body, snapshot_id.clone(), &name) {
            Ok(record) => record,
            Err(err) => {
                return Ok(Self::client_or_server_response(
                    err,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let record = match self.snapshot_manager.create(record).await {
            Ok(record) => record,
            Err(err) => {
                let error = Self::bad_request_for_repository_build_error(&err)
                    .unwrap_or_else(|| Self::repository_error(&err));
                return Ok(Self::client_or_server_response(
                    error,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let response = models::TemplateRequestResponseV3::new(
            record.id.to_string(),
            record.id.to_string(),
            true,
            vec![name.clone()],
            Vec::new(),
            vec![name],
        );

        Ok(V3TemplatesPostResponse::Status202_TheBuildWasRequestedSuccessfully(response))
    }

    async fn v2_templates_template_id_builds_build_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::V2TemplatesTemplateIdBuildsBuildIdPostPathParams,
        body: &models::TemplateBuildStartV2,
    ) -> Result<V2TemplatesTemplateIdBuildsBuildIdPostResponse, ()> {
        // 🔴 First, before the request is read at all, because the answer does
        // not depend on the request. A build drives a real Firecracker VM from
        // `TemplateBuildRunner`, outside the orchestrator and therefore outside
        // everything else the split made remote — but unlike a cold create,
        // this one *can* be forwarded: `TemplateBuildRunner` is ordinary Rust
        // that runs wherever it is called, and a node has `/dev/kvm`, `regctl`
        // and a Firecracker binary even when this process does not. So this
        // half does not refuse here — it dispatches to a node instead
        // (`run_the_build_on_a_node`, below). What is refused is the one
        // configuration that can do nothing at all: no node placement to send
        // the build to, which today only happens if `assemble_api` is ever
        // changed to construct an `ApiImpl` without one.
        //
        // 🔴 This condition used to also spare a process that could run the
        // build itself (`!self.runs_sandbox_runtime() && ...`). That half of it
        // is gone with `run_the_build_locally`: there is no local arm left to
        // spare, and a build with nowhere to dispatch is refused whichever
        // binary is asked. The refusal itself is deliberately *not* dropped —
        // see below for what it costs to admit a build nothing will run.
        //
        // Refusing here is not a smaller version of running it elsewhere. It is
        // the whole difference between an answer the caller gets and an answer
        // only a log has. Nothing has been mutated at this point, so the
        // template row is left exactly as it was found, in `waiting`.
        if self.node_placement().is_none() {
            warn!(
                template_id = %path_params.template_id,
                "refused a template build: this process has no node placement source to forward \
                 the build to"
            );
            return Ok(v2_start_build_error(Self::error(
                // 🔴 500 because it is the only code this operation declares
                // that means "this server, not your request"
                // (`src/api/openapi.yml`: 202/400/401/404/500). 501 and 503 say
                // it better and neither is in the schema, and inventing one
                // here would mean a body whose `code` and whose HTTP status
                // disagree — `v2_start_build_error` maps anything unrecognised
                // to 500 regardless.
                500,
                "template builds are not available on this replica: it runs no sandbox runtime \
                 of its own and has no node placement source configured to forward the build to. \
                 The template was left untouched and is still waiting to be built.",
            )));
        }

        if path_params.template_id != path_params.build_id {
            return Ok(v2_start_build_error(Self::error(
                400,
                "templateID and buildID must match in AgentENV compatibility mode",
            )));
        }

        let build_id = match SnapshotId::parse(path_params.build_id.as_str()) {
            Ok(id) => id,
            Err(_) => {
                return Ok(v2_start_build_error(Self::error(
                    400,
                    format!("invalid buildID: {}", path_params.build_id),
                )));
            }
        };
        // 🔴 Every row, and the name of the binding says why: the record this
        // reads is *pending*. A template is `waiting` until a build commits,
        // and this is the call that starts that build — so at the resolvable
        // scope it 404s on the one state it is guaranteed to find, and the
        // template can never leave `waiting`.
        let pending_record = match self
            .snapshot_manager
            .get_scoped(&path_params.template_id, CatalogReadScope::AnyStatus)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(
                    V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(
                        Self::error(
                            404,
                            format!("template {} not found", path_params.template_id),
                        ),
                    ),
                );
            }
            Err(err) => {
                return Ok(v2_start_build_error(Self::snapshot_manager_error(&err)));
            }
        };
        if pending_record.id != build_id {
            return Ok(v2_start_build_error(Self::error(
                400,
                "templateID must reference the same build record as buildID",
            )));
        }
        let base_source = match template_build_start_base_source(body) {
            Ok(source) => source,
            Err(err) => return Ok(v2_start_build_error(err)),
        };
        let spec = match template_build_spec_from_start_request(
            body,
            pending_record.alias.as_ref(),
            pending_record.resources,
        ) {
            Ok(spec) => spec,
            Err(err) => return Ok(v2_start_build_error(err)),
        };

        let started = match self.snapshot_manager.try_start_build(&build_id).await {
            Ok(started) => started,
            Err(crate::snapshot::RepositoryError::SnapshotNotFound { .. }) => {
                return Ok(
                    V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(
                        Self::error(
                            404,
                            format!("template {} not found", path_params.template_id),
                        ),
                    ),
                );
            }
            Err(err) => {
                return Ok(v2_start_build_error(Self::repository_error(&err)));
            }
        };

        let api = self.clone();
        tokio::spawn(async move {
            info!(build_id = %build_id, "template build started");
            // 🔴 The build's own id, not the template's. The catalog keys a
            // build row by it, and they are only equal for a backend that has
            // no build rows to renew against.
            let lease_build_id = started.build_id;
            let lease_api = api.clone();
            let build = run_the_build_on_a_node(api, build_id, base_source, spec);
            hold_the_build_lease(&lease_api, &lease_build_id, build).await;
        });

        Ok(V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status202_TheBuildHasStarted)
    }
}

/// Runs a build while telling the catalog this node is still on it.
///
/// 🔴 Two things a build needs that the build itself cannot do. The catalog's
/// reaper ends a build it has not heard from within the heartbeat TTL, so
/// without the renewals below every build longer than the TTL is taken away
/// mid-run and its template handed to whoever asks next. And when the lease
/// *is* lost, the renewal's answer is the only notice this process gets — so it
/// has to act on it here, because nothing downstream will.
async fn hold_the_build_lease(
    api: &ApiImpl,
    build_id: &SnapshotId,
    build: impl std::future::Future<Output = ()>,
) {
    let interval = std::time::Duration::from_secs(
        crate::cfg::ConfigManager::global_config()
            .snapshot
            .catalog
            .build_heartbeat_interval_secs
            .max(1),
    );
    hold_a_lease(build_id, interval, build, || {
        api.snapshot_manager.renew_build_lease(build_id)
    })
    .await
}

/// [`hold_the_build_lease`] with the renewal passed in.
///
/// 🔴 Split out so the decision can be tested without an `ApiImpl` behind it.
/// What has to be held still here is which of three answers stops a build, and
/// that is exactly the part a test built around a whole API implementation
/// cannot reach: the interesting case is a catalog answering `false`, which no
/// real catalog does on demand.
async fn hold_a_lease<Renew, Answer>(
    build_id: &SnapshotId,
    interval: std::time::Duration,
    build: impl std::future::Future<Output = ()>,
    mut renew: Renew,
) where
    Renew: FnMut() -> Answer,
    Answer: std::future::Future<Output = crate::snapshot::RepositoryResult<bool>>,
{
    tokio::pin!(build);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate; the catalog stamped a heartbeat when it
    // admitted the build, so there is nothing to say yet.
    ticker.tick().await;

    // Whether the catalog has ever confirmed this build is ours. See the arm
    // below that reads it.
    let mut held_once = false;

    loop {
        tokio::select! {
            () = &mut build => return,
            _ = ticker.tick() => match renew().await {
                Ok(true) => held_once = true,
                // 🔴 Only once a renewal has succeeded. Before that, "not the
                // live build" means the catalog has no row for it — which is
                // what an admission the catalog was unreachable for looks like
                // until the compensator replays it, and stopping the build over
                // that would make a scheduler blip destroy work. It cannot hide
                // a real reaping: the admitting statement stamps a heartbeat,
                // so a build can only be reaped a full TTL after it starts, by
                // which time renewals at a third of the TTL have long since
                // succeeded.
                Ok(false) if !held_once => {
                    warn!(
                        build_id = %build_id,
                        "the catalog has no row for this build yet; carrying on, because an \
                         admission it could not take is replayed rather than lost"
                    );
                }
                Ok(false) => {
                    // 🔴 Stop, and write nothing. The template this build was
                    // holding has already been handed to somebody else — the
                    // reaper freed it and recorded why, or another build now
                    // owns it — so marking it failed from here would either
                    // overwrite that reason or fail a build that is not this
                    // one. Dropping the future is what stops it.
                    warn!(
                        build_id = %build_id,
                        "this build's lease is gone: the catalog has handed its template to \
                         somebody else, so the build is being stopped. Nothing is written about \
                         the template — whatever holds it now is not this build"
                    );
                    return;
                }
                Err(error) => {
                    // A scheduler nobody can reach is not a reason to throw
                    // away a build that is running. If the outage outlasts the
                    // TTL the reaper takes the template and the next renewal
                    // says so, which is the branch above.
                    warn!(
                        build_id = %build_id,
                        error = %format_args!("{error:#}"),
                        "could not renew this build's lease; carrying on, but the catalog will \
                         reap the build if this lasts longer than its heartbeat TTL"
                    );
                }
            },
        }
    }
}

/// Runs a template build: picks a node through `api.node_placement()`, asks it
/// to run the build (`crate::node_client::build_template_on_a_node`), and
/// commits what comes back. Every failure ends the same way —
/// `mark_v2_build_error` on `build_id`, and the template row stays `waiting`
/// for the reaper to hand to another build if nothing else claims it first.
///
/// 🔴 The only arm. A `run_the_build_locally` sibling used to drive
/// `TemplateBuildRunner` in this process, chosen by
/// `ApiImpl::runs_sandbox_runtime`; it is deleted. `aenv-api` never took it,
/// and `aenv-node` never reaches this route at all — `crate::api::role_gate`
/// answers the user-facing REST surface with 404 there, and a node runs builds
/// through the gRPC `NodeSandboxService::build_template`, which does not go
/// through `ApiImpl`.
///
/// # 🔴 Alias ownership
///
/// The node never applies an alias when it stages this build — see
/// `TemplateBuildRequest`'s doc in `node.proto`. This is where the alias this
/// build actually gets is decided: `adopted_build_metadata` builds a
/// `SnapshotPublishMetadata` carrying `spec`'s own alias, and
/// `SnapshotManager::publish_captured` -> `stage_captured` -> `adopt_staged`
/// writes it into `staged.commit.alias` unconditionally, discarding whatever
/// (nothing) the node wrote there. This is not a new rule invented for
/// template builds — it is the same rule a published pause capture already
/// follows (`capture_publish_metadata(metadata, None)` at the node, the real
/// alias applied by the committer in `stage_for_caller`'s caller), reused
/// unchanged because the reason is identical: alias names a row in *this
/// process's* catalog, and only the process that owns the catalog gets to
/// decide what a row is called.
async fn run_the_build_on_a_node(
    api: ApiImpl,
    build_id: SnapshotId,
    base_source: TemplateBuildStartBaseSource,
    spec: crate::template::TemplateBuildSpec,
) {
    let Some(placement) = api.node_placement() else {
        // 🔴 Loud rather than silent: there is no local run to fall back to.
        // See the door refusal in
        // `v2_templates_template_id_builds_build_id_post`, which is meant to
        // catch this before a build is ever admitted. Reaching here means that
        // refusal's premise changed without this arm changing with it.
        warn!(
            build_id = %build_id,
            "template build failed: this replica has no node placement source configured"
        );
        mark_v2_build_error(
            &api,
            &build_id,
            TemplateBuildErrorReason::new(
                "this replica has no node placement source configured and cannot run a template \
                 build remotely",
            ),
        )
        .await;
        return;
    };

    let resources = match spec.resources_ref().copied() {
        Some(resources) => resources,
        None => {
            mark_v2_build_error(
                &api,
                &build_id,
                TemplateBuildErrorReason::new("template build spec has no resources set"),
            )
            .await;
            return;
        }
    };

    let request =
        match build_template_wire_request(&api, &build_id, &base_source, &spec, resources).await {
            Ok(request) => request,
            Err(reason) => {
                mark_v2_build_error(&api, &build_id, reason).await;
                return;
            }
        };

    let staged =
        match crate::node_client::build_template_on_a_node(placement.as_ref(), resources, request)
            .await
        {
            Ok(staged) => staged,
            Err(reason) => {
                warn!(
                    build_id = %build_id,
                    reason = %reason.message,
                    failed_step = ?reason.step,
                    "template build failed while running on a node"
                );
                mark_v2_build_error(&api, &build_id, reason).await;
                return;
            }
        };

    let alias = match spec.parsed_alias() {
        Ok(alias) => alias,
        Err(err) => {
            mark_v2_build_error(
                &api,
                &build_id,
                TemplateBuildErrorReason::new(err.to_string()),
            )
            .await;
            return;
        }
    };

    let metadata = adopted_build_metadata(&build_id, alias);
    let captured = CapturedSandboxSnapshot::staged(staged);
    match api
        .snapshot_manager
        .publish_captured(metadata, captured)
        .await
    {
        Ok(record) => {
            info!(build_id = %build_id, snapshot_id = %record.id, "template build completed");
        }
        Err(error) => {
            mark_v2_build_error(
                &api,
                &build_id,
                TemplateBuildErrorReason::new(format!("commit staged template build: {error}")),
            )
            .await;
        }
    }
}

/// Turns a `TemplateBuildSpec` and its base source into the wire request
/// `NodeSandboxService::build_template` accepts.
///
/// 🔴 `Template(alias)` resolves the base's catalog row here, before
/// dispatch — the read Q3 moved off the node (see
/// `docs/proposals/_sd-phase4-open-questions-resolved.md` Q3 and
/// `TemplateBuildRequest.base_snapshot_resolved`'s own doc in node.proto).
/// Only the row: `api.snapshot_manager.get`, never `load_runnable` — local
/// artifact resolution belongs to whichever machine boots the build sandbox,
/// and that machine is the node this request is about to be sent to, not
/// this replica.
async fn build_template_wire_request(
    api: &ApiImpl,
    build_id: &SnapshotId,
    base_source: &TemplateBuildStartBaseSource,
    spec: &crate::template::TemplateBuildSpec,
    resources: SandboxResources,
) -> Result<pb::TemplateBuildRequest, TemplateBuildErrorReason> {
    let steps = pb::encode_value(&spec.steps().to_vec()).map_err(|err| {
        TemplateBuildErrorReason::new(format!("encode template build steps: {err}"))
    })?;
    let mut base_snapshot_resolved = None;
    let base = match base_source {
        TemplateBuildStartBaseSource::DefaultImage => {
            pb::template_build_request::Base::Image(pb::TemplateBuildImageBase {
                image_ref: String::new(),
            })
        }
        TemplateBuildStartBaseSource::Image(image_ref) => {
            pb::template_build_request::Base::Image(pb::TemplateBuildImageBase {
                image_ref: image_ref.clone(),
            })
        }
        TemplateBuildStartBaseSource::Template(alias) => {
            let record = match api.snapshot_manager.get(alias.as_ref()).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    return Err(TemplateBuildErrorReason::new(format!(
                        "template alias not found: {alias}"
                    )));
                }
                Err(err) => {
                    return Err(TemplateBuildErrorReason::new(
                        ApiImpl::snapshot_manager_error(&err).message,
                    ));
                }
            };
            base_snapshot_resolved = Some(pb::encode_value(&record).map_err(|err| {
                TemplateBuildErrorReason::new(format!("encode base template record: {err}"))
            })?);
            pb::template_build_request::Base::BaseSnapshotRef(alias.to_string())
        }
    };
    Ok(pb::TemplateBuildRequest {
        build_snapshot_id: build_id.to_string(),
        base: Some(base),
        base_snapshot_resolved,
        steps: Some(steps),
        resources: Some(pb::SandboxResources {
            cpu_count: resources.cpu_count,
            memory_mib: resources.memory_mib,
            disk_size_mib: resources.disk_size_mib,
        }),
        start_cmd: spec.start_cmd_ref().unwrap_or_default().to_string(),
        ready_cmd: spec.ready_cmd_ref().unwrap_or_default().to_string(),
    })
}

/// A `SnapshotPublishMetadata` for `adopt_staged` to apply over a node-staged
/// template build.
///
/// 🔴 Only two of its fields ever reach `adopt_staged`: `source`, checked
/// against the row the node staged (both sides are always
/// `SnapshotPublishSource::Template`, so this check can never fail here the
/// way it can for a sandbox capture), and `alias`, applied unconditionally.
/// Everything else is discarded — `adopt_staged` commits the *node's*
/// richly-populated row, never this one's — so the placeholders below are
/// never read by anything.
fn adopted_build_metadata(
    build_id: &SnapshotId,
    alias: Option<SnapshotAlias>,
) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id: build_id.clone(),
        alias,
        source: SnapshotPublishSource::Template,
        context: CommandContext::default(),
        startup: None,
        resources: SandboxResources::default(),
        runtime_versions: SnapshotRuntimeVersions {
            kernel_version: String::new(),
            firecracker_version: String::new(),
            envd_version: String::new(),
            tools_drive_version: String::new(),
        },
        virtualization_mode: VirtualizationMode::default(),
        image_configs: ImageConfigs::new(),
        custom_extension_params: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{hold_a_lease, SnapshotId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const TICK: Duration = Duration::from_secs(100);

    /// A build that runs until told to stop, and says whether it finished.
    ///
    /// 🔴 The distinction the lease tests turn on is "did the build get to
    /// run to the end", so it has to be a distinction the fixture can express
    /// in both directions. A build future that always completes immediately
    /// would make every one of these tests pass whatever the loop did.
    async fn a_build(seconds: u64, finished: Arc<AtomicUsize>) {
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        finished.fetch_add(1, Ordering::SeqCst);
    }

    /// The control for every test below: a build that outlives several ticks
    /// keeps running, and each tick is one renewal.
    #[tokio::test(start_paused = true)]
    async fn a_live_lease_lets_the_build_run_to_the_end() {
        let finished = Arc::new(AtomicUsize::new(0));
        let renewals = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renewals);

        hold_a_lease(
            &SnapshotId::generate(),
            TICK,
            a_build(350, Arc::clone(&finished)),
            move || {
                let counted = Arc::clone(&counted);
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
        )
        .await;

        assert_eq!(finished.load(Ordering::SeqCst), 1, "the build finished");
        assert_eq!(
            renewals.load(Ordering::SeqCst),
            3,
            "a build spanning three intervals renews three times"
        );
    }

    /// 🔴 A lease that is gone stops the build. The template has been handed to
    /// somebody else, and this is the only notice this process gets.
    ///
    /// The first renewal has to succeed for the loss to count — see the test
    /// below — so this one holds the lease once and then loses it.
    #[tokio::test(start_paused = true)]
    async fn a_lost_lease_stops_the_build() {
        let finished = Arc::new(AtomicUsize::new(0));
        let renewals = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renewals);

        hold_a_lease(
            &SnapshotId::generate(),
            TICK,
            a_build(350, Arc::clone(&finished)),
            move || {
                let counted = Arc::clone(&counted);
                async move { Ok(counted.fetch_add(1, Ordering::SeqCst) == 0) }
            },
        )
        .await;

        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "the build must not have run to the end: its template is somebody else's now"
        );
        assert_eq!(
            renewals.load(Ordering::SeqCst),
            2,
            "it held the lease once and stopped on the renewal that lost it"
        );
    }

    /// 🔴 A build whose admission the catalog could not take must not be
    /// stopped by the catalog not knowing about it.
    ///
    /// An unreachable catalog leaves the admission queued rather than lost, so
    /// until the compensator replays it every renewal answers "not the live
    /// build" — which is the same answer a reaped build gets. Treating them the
    /// same would make a scheduler blip during admission destroy the build it
    /// admitted. They are told apart by whether a renewal has ever succeeded,
    /// and that is safe because the admitting statement stamps a heartbeat: a
    /// real reaping cannot happen before a full TTL, by which time renewals at
    /// a third of the TTL have succeeded.
    #[tokio::test(start_paused = true)]
    async fn a_build_the_catalog_has_not_heard_of_yet_keeps_running() {
        let finished = Arc::new(AtomicUsize::new(0));

        hold_a_lease(
            &SnapshotId::generate(),
            TICK,
            a_build(350, Arc::clone(&finished)),
            || async { Ok(false) },
        )
        .await;

        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "a build the catalog has never confirmed must not be stopped by that"
        );
    }

    /// A catalog nobody can reach is not a lost lease. Throwing away a build
    /// that is running perfectly well because the scheduler is restarting is
    /// the failure the renewal exists to avoid, not one to add.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_catalog_does_not_stop_the_build() {
        let finished = Arc::new(AtomicUsize::new(0));

        hold_a_lease(
            &SnapshotId::generate(),
            TICK,
            a_build(350, Arc::clone(&finished)),
            || async {
                Err(crate::snapshot::RepositoryError::Backend {
                    message: "the scheduler is restarting".to_string(),
                    source: None,
                })
            },
        )
        .await;

        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "a build must survive a catalog it cannot reach"
        );
    }

    /// A build shorter than one interval never renews. The catalog stamped a
    /// heartbeat when it admitted it, so there is nothing to say.
    #[tokio::test(start_paused = true)]
    async fn a_build_shorter_than_the_interval_never_renews() {
        let finished = Arc::new(AtomicUsize::new(0));
        let renewals = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renewals);

        hold_a_lease(
            &SnapshotId::generate(),
            TICK,
            a_build(1, Arc::clone(&finished)),
            move || {
                let counted = Arc::clone(&counted);
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
        )
        .await;

        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(renewals.load(Ordering::SeqCst), 0);
    }
}

/// 🔴 The template surface, read through a catalog that hides what PostgreSQL
/// hides.
///
/// Every endpoint here is asking about a row that is deliberately *not*
/// resolvable: a template is `waiting` from the moment it is created until its
/// first build commits, and the central catalog answers a resolvable read with
/// `status_group = 'ready'`. Read at that scope the whole surface goes dark —
/// 404 from the get, absent from both listings, 404 from the build start that
/// would have moved the row out of `waiting`, and a delete that reports 204
/// having deleted nothing. That is what a cluster on `read = postgres` did,
/// and each test below is one of those endpoints.
///
/// The double is what makes them fail rather than pass by accident: a catalog
/// that answered both scopes the same way — every object-store catalog does —
/// agrees with a handler that asks at the wrong one.
#[cfg(test)]
mod template_read_scope_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::templates::*;
    use agentenv_http_server::models;

    use super::{run_the_build_on_a_node, ApiImpl, TemplateBuildStartBaseSource};
    use crate::identity::NodeIdentity;
    use crate::node_client::{FixedNodePlacement, NodeEndpoint, NodePlacement};
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator,
    };
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::repository::interfaces::{
        SnapshotCatalog, SnapshotCommit, SnapshotListPage, StartedBuild,
    };
    use crate::snapshot::repository::{
        RepositoryError, RepositoryResult, SnapshotListFilter, SnapshotRepository,
    };
    use crate::snapshot::{
        CatalogReadScope, SnapshotAlias, SnapshotId, SnapshotManager, SnapshotRecord,
        SnapshotSource, TemplateBuildErrorReason, TemplateBuildInfo,
    };
    use crate::template::TemplateBuildSpec;

    /// A catalog that hides a `waiting` row from a resolvable read, and shows
    /// it to a scoped one. The central catalog, in the one respect these tests
    /// are about.
    struct PendingTemplateCatalog {
        record: SnapshotRecord,
        deletes: Arc<AtomicUsize>,
        build_starts: Arc<AtomicUsize>,
    }

    impl PendingTemplateCatalog {
        fn visible_at(&self, scope: CatalogReadScope) -> bool {
            matches!(scope, CatalogReadScope::AnyStatus)
        }
    }

    #[async_trait]
    impl SnapshotCatalog for PendingTemplateCatalog {
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
            Ok(None)
        }

        async fn get_scoped(
            &self,
            id_or_alias: &str,
            scope: CatalogReadScope,
        ) -> RepositoryResult<Option<SnapshotRecord>> {
            let names_it = id_or_alias == self.record.id.to_string()
                || self
                    .record
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.as_ref() == id_or_alias);
            Ok((self.visible_at(scope) && names_it).then(|| self.record.clone()))
        }

        /// 🔴 Not hard-coded empty: routed through the scoped listing below at
        /// the scope an unscoped read means. A `waiting` template is invisible
        /// at `Resolvable`, so this comes back empty — but it comes back empty
        /// for the reason the production catalog would, and it starts returning
        /// the row the moment `visible_at` says it should.
        async fn list_page(
            &self,
            filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            self.list_page_scoped(filter, CatalogReadScope::Resolvable)
                .await
        }

        async fn list_page_scoped(
            &self,
            _filter: SnapshotListFilter,
            scope: CatalogReadScope,
        ) -> RepositoryResult<SnapshotListPage> {
            Ok(SnapshotListPage {
                items: if self.visible_at(scope) {
                    vec![self.record.clone()]
                } else {
                    Vec::new()
                },
                next: None,
            })
        }

        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn resolve_alias_scoped(
            &self,
            alias: &str,
            scope: CatalogReadScope,
        ) -> RepositoryResult<Option<SnapshotId>> {
            let names_it = self
                .record
                .alias
                .as_ref()
                .is_some_and(|bound| bound.as_ref() == alias);
            Ok((self.visible_at(scope) && names_it).then(|| self.record.id.clone()))
        }

        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            self.build_starts.fetch_add(1, Ordering::SeqCst);
            // Reaching this call is the whole assertion; what it answers only
            // has to be something that is not a 404, so that a test cannot
            // pass by getting the right status code down the wrong road.
            Err(RepositoryError::Backend {
                message: "the admission is not what this test is about".to_string(),
                source: None,
            })
        }

        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            Ok(())
        }
    }

    /// What every test here reads: the surface, one template on it that has
    /// never been built, and the two counters.
    struct Surface {
        api: Arc<ApiImpl>,
        id: SnapshotId,
        alias: SnapshotAlias,
        deletes: Arc<AtomicUsize>,
        build_starts: Arc<AtomicUsize>,
    }

    /// The surface every read-scope fixture here uses: one that can dispatch a
    /// build, so the build route's door refusal is not what they measure.
    async fn surface() -> Surface {
        surface_as(Some(unreachable_placement())).await
    }

    /// The same surface, differing in one value: where this half sends a
    /// template build, or `None` for the misconfiguration that has nowhere to
    /// send one.
    ///
    /// 🔴 The `ApiImpl` is always the deciding half
    /// (`ResumeWiring::api_half_for_test`). It used to be a parameter, because
    /// four user-facing routes forked on `ApiImpl::runs_sandbox_runtime` and
    /// these fixtures had to drive both arms; those forks are collapsed and
    /// `aenv-node` answers this whole route group with 404
    /// (`crate::api::role_gate`), so the running half never reaches any handler
    /// under test here.
    ///
    /// 🔴 The orchestrator takes [`AccessTokenSeedPolicy::MayGenerate`] in
    /// every case. It is not what the build route reads — the handler asks
    /// `ApiImpl` — and an `Orchestrator` built with `MustBeConfigured` refuses
    /// to construct without a configured envd access-token seed, which would
    /// make the refusing half of these tests fail on the fixture rather than
    /// on the thing under test.
    async fn surface_as(node_placement: Option<Arc<dyn NodePlacement>>) -> Surface {
        let id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("pending-template").expect("alias parses");
        let now = 1_700_000_000_000;
        let record = SnapshotRecord {
            id: id.clone(),
            alias: Some(alias.clone()),
            // 🔴 `waiting`, which is every template between its creation and
            // its first commit — the state the whole defect is about.
            source: SnapshotSource::Template {
                build: TemplateBuildInfo::waiting(),
            },
            resources: Default::default(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            committed: None,
        };

        let deletes = Arc::new(AtomicUsize::new(0));
        let build_starts = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(PendingTemplateCatalog {
            record,
            deletes: Arc::clone(&deletes),
            build_starts: Arc::clone(&build_starts),
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
        // Held for the process's lifetime: the persister above keeps reading it.
        std::mem::forget(root);

        let snapshot_manager = Arc::new(SnapshotManager::from_parts(
            Arc::new(SnapshotRepository::new(
                catalog,
                Arc::new(crate::snapshot::mock::MockSnapshotArtifactStore),
            )),
            Some(Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver)),
            None,
        ));

        let api = ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(DisabledPausedSandboxRegistry),
                Arc::clone(&snapshot_manager),
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        );
        // 🔴 A builder step, matching `assemble_api`'s own use of it — see
        // `ApiImpl::with_node_placement`.
        let api = Arc::new(match node_placement {
            Some(placement) => api.with_node_placement(placement),
            None => api,
        });

        Surface {
            api,
            id,
            alias,
            deletes,
            build_starts,
        }
    }

    fn claims() -> super::super::Claims {
        super::super::Claims
    }

    fn host() -> Host {
        Host::from(http::uri::Authority::from_static("localhost"))
    }

    /// `POST /v2/templates/{id}/builds/{id}` against a surface, with the one
    /// body every one of these fixtures sends.
    async fn start_a_build(s: &Surface) -> V2TemplatesTemplateIdBuildsBuildIdPostResponse {
        s.api
            .v2_templates_template_id_builds_build_id_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::V2TemplatesTemplateIdBuildsBuildIdPostPathParams {
                    template_id: s.id.to_string(),
                    build_id: s.id.to_string(),
                },
                &models::TemplateBuildStartV2::new(),
            )
            .await
            .expect("the handler answers")
    }

    /// The message a role refusal carries, or `None` when the handler did not
    /// refuse on the role.
    ///
    /// 🔴 The status code alone cannot tell the two apart, and that is a fact
    /// about the API rather than about this helper: `/v2/templates/{id}/builds/
    /// {id}` declares 202/400/401/404/500 and nothing else, so the refusal has
    /// to reuse 500 — which is also what a failed build admission answers. What
    /// separates them is the text, so the text is what this reads.
    fn role_refusal(response: &V2TemplatesTemplateIdBuildsBuildIdPostResponse) -> Option<&str> {
        match response {
            V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status500_ServerError(error)
                if error
                    .message
                    .contains("no node placement source configured") =>
            {
                Some(error.message.as_str())
            }
            _ => None,
        }
    }

    /// `GET /templates/{id}`.
    #[tokio::test]
    async fn a_template_that_has_never_been_built_can_still_be_fetched() {
        let s = surface().await;
        let response = s
            .api
            .templates_template_id_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::TemplatesTemplateIdGetPathParams {
                    template_id: s.id.to_string(),
                },
                &models::TemplatesTemplateIdGetQueryParams {
                    limit: None,
                    next_token: None,
                },
            )
            .await
            .expect("the handler answers");

        assert!(
            matches!(
                response,
                TemplatesTemplateIdGetResponse::Status200_SuccessfullyReturnedTheTemplateWithItsBuilds(_)
            ),
            "a pending template must be visible to the endpoint that reports its build state, \
             got {response:?}"
        );
    }

    /// `GET /templates` and `GET /v2/templates`.
    #[tokio::test]
    async fn both_listings_show_a_template_that_has_never_been_built() {
        let s = surface().await;

        let v1 = s
            .api
            .templates_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::TemplatesGetQueryParams {
                    team_id: None,
                    limit: None,
                    next_token: None,
                },
            )
            .await
            .expect("the handler answers");
        match v1 {
            TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates { body, .. } => {
                assert_eq!(
                    body.len(),
                    1,
                    "a template is created `waiting`, so a listing that cannot see `waiting` \
                     cannot see a newly created template"
                );
            }
            other => panic!("the listing must succeed, got {other:?}"),
        }

        let v2 = s
            .api
            .v2_templates_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::V2TemplatesGetQueryParams {
                    team_id: None,
                    limit: None,
                    next_token: None,
                },
            )
            .await
            .expect("the handler answers");
        match v2 {
            V2TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates { body, .. } => {
                assert_eq!(
                    body.len(),
                    1,
                    "and the v2 listing is the same endpoint twice"
                );
            }
            other => panic!("the v2 listing must succeed, got {other:?}"),
        }
    }

    /// `GET /templates/{id}/builds/{id}/status` — the endpoint a client polls.
    #[tokio::test]
    async fn the_build_status_endpoint_reports_a_build_that_has_not_finished() {
        let s = surface().await;
        let response = s
            .api
            .templates_template_id_builds_build_id_status_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::TemplatesTemplateIdBuildsBuildIdStatusGetPathParams {
                    template_id: s.id.to_string(),
                    build_id: s.id.to_string(),
                },
            )
            .await
            .expect("the handler answers");

        assert!(
            matches!(
                response,
                TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status200_SuccessfullyReturnedTheTemplate(_)
            ),
            "the states this endpoint exists to report are exactly the ones a resolvable read \
             hides, got {response:?}"
        );
    }

    /// `GET /templates/aliases/{alias}` — how `aenv` turns a name into an id.
    #[tokio::test]
    async fn a_pending_template_can_be_found_by_the_name_it_was_created_with() {
        let s = surface().await;
        let response = s
            .api
            .templates_aliases_alias_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::TemplatesAliasesAliasGetPathParams {
                    alias: s.alias.to_string(),
                },
            )
            .await
            .expect("the handler answers");

        assert!(
            matches!(
                response,
                TemplatesAliasesAliasGetResponse::Status200_SuccessfullyQueriedTemplateByAlias(_)
            ),
            "every id-or-name argument goes through this endpoint, got {response:?}"
        );
    }

    /// `POST /v2/templates/{id}/builds/{id}` — the call that would move the row
    /// out of `waiting` in the first place.
    #[tokio::test]
    async fn a_build_can_be_started_on_a_template_that_has_never_been_built() {
        let s = surface().await;
        let response = s
            .api
            .v2_templates_template_id_builds_build_id_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::V2TemplatesTemplateIdBuildsBuildIdPostPathParams {
                    template_id: s.id.to_string(),
                    build_id: s.id.to_string(),
                },
                &models::TemplateBuildStartV2::new(),
            )
            .await
            .expect("the handler answers");

        assert!(
            !matches!(
                response,
                V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(_)
            ),
            "the record this reads is named `pending` because it is: refusing it as absent is \
             what left the template unable to leave `waiting`, got {response:?}"
        );
        assert_eq!(
            s.build_starts.load(Ordering::SeqCst),
            1,
            "and the read must have got far enough to ask for the admission"
        );
    }

    /// A placement source a build can be sent to — its address is never
    /// dialled by the tests that use it below, which only check whether the
    /// *door* let the build past the refusal.
    fn unreachable_placement() -> Arc<dyn NodePlacement> {
        Arc::new(FixedNodePlacement::new(NodeEndpoint::same_address(
            "unreachable-test-node",
            "http://127.0.0.1:1",
        )))
    }

    /// 🔴 A process with no node to send the build to is refused at the door —
    /// and the point of this test is that this is the *only* configuration
    /// that is, which `assemble_api` never produces in production.
    ///
    /// Before `run_the_build_on_a_node` existed, `aenv-api` refused
    /// unconditionally: the 202 would otherwise have gone out, the build would
    /// have died in a background task with no machine to run it on, and the
    /// status endpoint would have reported the same generic reason a user's
    /// broken `RUN` step reports. This is the regression that refusal existed
    /// to prevent, restated as "still true when there is truly nowhere to
    /// send the build" rather than "true for `aenv-api` unconditionally".
    ///
    /// 🔴 A second arm used to assert that a `ResumeWiring::node_local`
    /// surface was admitted without a placement, because it could run the
    /// build in-process. `run_the_build_locally` is deleted — there is no
    /// in-process arm to be spared by — and its opposite,
    /// [`an_api_replica_with_a_node_to_send_the_build_to_is_admitted`], is what
    /// keeps this from passing on a tree that refuses every build.
    ///
    /// The discriminator is the admission counter rather than the status
    /// code, because a refusal and a failed admission are both 500 — only one
    /// of them got as far as asking the catalog to start a build.
    #[tokio::test]
    async fn a_build_is_refused_only_when_it_can_run_nowhere_at_all() {
        let s = surface_as(None).await;
        let response = start_a_build(&s).await;
        let refusal = role_refusal(&response).unwrap_or_else(|| {
            panic!(
                "aenv-api with no node placement source has no /dev/kvm and nowhere to send \
                 the build, and answering anything but a refusal here is what made the failure \
                 arrive minutes later as a log line, got {response:?}"
            )
        });
        assert!(
            refusal.contains("no sandbox runtime"),
            "a refusal that does not say why must not be mistaken for a generic 500, got \
             {refusal:?}"
        );
        assert_eq!(
            s.build_starts.load(Ordering::SeqCst),
            0,
            "🔴 and it must refuse before the admission: a build the catalog has admitted is a \
             template moved out of `waiting` into a `building` state nothing will ever finish"
        );
    }

    /// 🔴 The regression guard for the gap this whole feature closes:
    /// a replica given a node to send the build to is admitted, not refused the
    /// way `aenv-api` always was before `run_the_build_on_a_node` existed.
    /// Turning the door's condition back into an unconditional refusal turns
    /// this assertion red — every build would be refused regardless of whether
    /// there is somewhere to send one.
    #[tokio::test]
    async fn an_api_replica_with_a_node_to_send_the_build_to_is_admitted() {
        let s = surface_as(Some(unreachable_placement())).await;
        let response = start_a_build(&s).await;
        assert!(
            role_refusal(&response).is_none(),
            "a replica with a node placement source can dispatch the build remotely and must \
             not be refused at the door, got {response:?}"
        );
        assert_eq!(
            s.build_starts.load(Ordering::SeqCst),
            1,
            "it must reach the admission exactly like aenv-node does"
        );
    }

    /// 🔴 The requirement this feature's build lease safety rests on: a node
    /// this process cannot reach must not leave `run_the_build_on_a_node`'s future
    /// pending forever. `hold_the_build_lease` only stops renewing once that
    /// future resolves — see `hold_a_lease`'s tests above, which cover *that*
    /// half generically against a synthetic build future — so what has to be
    /// true for a real remote build is that `run_the_build_on_a_node` itself
    /// resolves when the node cannot be reached. This drives it directly
    /// against a node nothing is listening on (a dial failure — the fast,
    /// common shape of "the node is gone"; a node that accepts the
    /// connection and then dies mid-build is bounded instead by
    /// `node_client::build`'s HTTP/2 keepalive and call timeout, which is a
    /// property of those constants rather than something a fast unit test
    /// can observe without waiting them out) and asserts it returns well
    /// inside a bound a build lease can survive.
    ///
    /// Replacing `CONNECT_TIMEOUT`'s use in `build_template_on_a_node` with
    /// an unbounded `.connect()` — or deleting the `.map_err` that turns a
    /// dial failure into an `Err` `run_the_build_on_a_node` can act on —
    /// turns this test red or makes it hang; either way it stops passing
    /// quietly.
    #[tokio::test]
    async fn a_node_nobody_answers_does_not_hang_the_remote_build() {
        let s = surface_as(Some(unreachable_placement())).await;
        let spec = TemplateBuildSpec::new().resources(1, 128);

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            run_the_build_on_a_node(
                (*s.api).clone(),
                s.id.clone(),
                TemplateBuildStartBaseSource::DefaultImage,
                spec,
            ),
        )
        .await;

        assert!(
            outcome.is_ok(),
            "a node refusing the connection must not hang the build lease loop: \
             run_the_build_on_a_node did not return within 15s"
        );
    }

    /// `DELETE /templates/{id}` — the failure that reports success.
    #[tokio::test]
    async fn deleting_a_template_that_has_never_been_built_deletes_it() {
        let s = surface().await;
        let response = s
            .api
            .templates_template_id_delete(
                &Method::DELETE,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::TemplatesTemplateIdDeletePathParams {
                    template_id: s.id.to_string(),
                },
            )
            .await
            .expect("the handler answers");

        assert!(
            matches!(
                response,
                TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully
            ),
            "got {response:?}"
        );
        assert_eq!(
            s.deletes.load(Ordering::SeqCst),
            1,
            "🔴 the 204 is not the assertion. A delete that cannot see the row finds nothing to \
             delete and reports success anyway, which is how a template survived being deleted \
             and went on holding its build's slot of the cluster ceiling"
        );
    }
}
