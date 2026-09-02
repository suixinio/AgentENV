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

        // Template-management lookups include non-ready lifecycle states.
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

    /// Lists a bounded page of templates across all lifecycle states.
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

        // Build-status reads include every template lifecycle state.
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
        // Template detail includes waiting, building, and error records.
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
        // Refuse before admission only when no node placement can run the build.
        if self.node_placement().is_none() {
            warn!(
                template_id = %path_params.template_id,
                "refused a template build: this process has no node placement source to forward \
                 the build to"
            );
            return Ok(v2_start_build_error(Self::error(
                // The schema's server-side failure response is 500.
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
        // Admission must read the pending, non-resolvable template row.
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
            // Renew under the admitted build id, which may differ from template id.
            let lease_build_id = started.build_id;
            let lease_api = api.clone();
            let build = run_the_build_on_a_node(api, build_id, base_source, spec);
            hold_the_build_lease(&lease_api, &lease_build_id, build).await;
        });

        Ok(V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status202_TheBuildHasStarted)
    }
}

/// Runs a build while renewing its catalog lease and stops if ownership is lost.
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

/// Testable lease loop with an injected renewal operation.
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
    // Admission already stamped the first heartbeat.
    ticker.tick().await;

    // A build is only considered established after one confirmed renewal.
    let mut held_once = false;

    loop {
        tokio::select! {
            () = &mut build => return,
            _ = ticker.tick() => match renew().await {
                Ok(true) => held_once = true,
                // Before first confirmation, a missing row may still be queued for replay.
                Ok(false) if !held_once => {
                    warn!(
                        build_id = %build_id,
                        "the catalog has no row for this build yet; carrying on, because an \
                         admission it could not take is replayed rather than lost"
                    );
                }
                Ok(false) => {
                    // Lost ownership stops the build without writing over its successor.
                    warn!(
                        build_id = %build_id,
                        "this build's lease is gone: the catalog has handed its template to \
                         somebody else, so the build is being stopped. Nothing is written about \
                         the template — whatever holds it now is not this build"
                    );
                    return;
                }
                Err(error) => {
                    // Temporary renewal outages do not stop a still-owned build.
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

/// Dispatches a template build to a selected node and commits the returned staging.
///
/// Alias ownership remains with this catalog-owning process.
async fn run_the_build_on_a_node(
    api: ApiImpl,
    build_id: SnapshotId,
    base_source: TemplateBuildStartBaseSource,
    spec: crate::template::TemplateBuildSpec,
) {
    let Some(placement) = api.node_placement() else {
        // This should have been refused before admission; fail loudly if reached.
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
        paused_sandbox: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{hold_a_lease, SnapshotId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const TICK: Duration = Duration::from_secs(100);

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
    use crate::node_client::{FixedNodePlacement, NodeEndpoint, NodePlacement};
    use crate::orchestrator::Orchestrator;
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

    async fn surface() -> Surface {
        surface_as(Some(unreachable_placement())).await
    }

    async fn surface_as(node_placement: Option<Arc<dyn NodePlacement>>) -> Surface {
        let id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("pending-template").expect("alias parses");
        let now = 1_700_000_000_000;
        let record = SnapshotRecord {
            id: id.clone(),
            alias: Some(alias.clone()),
            source: SnapshotSource::Template {
                build: TemplateBuildInfo::waiting(),
            },
            resources: Default::default(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            committed: None,
            origin_node_id: None,
        };

        let deletes = Arc::new(AtomicUsize::new(0));
        let build_starts = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(PendingTemplateCatalog {
            record,
            deletes: Arc::clone(&deletes),
            build_starts: Arc::clone(&build_starts),
        });

        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;

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
            snapshot_manager,
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        );
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
