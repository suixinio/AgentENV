use std::time::SystemTime;

use crate::types::SandboxId;
use agentenv_http_server::types::Nullable;
use agentenv_http_server::{apis::admin::*, models};

use super::super::paused;
use super::ApiImpl;

/// Renders an absent deadline as JSON null.
///
impl From<&paused::PausedSandboxRow> for models::RegistrySandbox {
    fn from(row: &paused::PausedSandboxRow) -> Self {
        models::RegistrySandbox::new(
            row.sandbox_id.to_string(),
            String::new(),
            "paused".to_string(),
            0,
            row.origin_node_id.clone().unwrap_or_default(),
            String::new(),
            row.snapshot_id.to_string(),
            row.origin_node_id.clone().unwrap_or_default(),
            row.paused_at_unix_ms,
            row.paused_at_unix_ms,
            Nullable::Null,
            Nullable::Null,
            String::new(),
        )
    }
}

/// Parses the keyset cursor: the sandbox id the previous page ended on.
///
/// A token that is not a sandbox id would compare against every row as an
/// arbitrary string and could silently end pagination early, so it is refused.
fn parse_page_token(raw: &str) -> Result<Option<SandboxId>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    SandboxId::parse_str(trimmed)
        .map(Some)
        .map_err(|_| "invalid nextToken".to_string())
}

/// The only state a paused sandbox can be in. Anything else names a state the
/// catalog does not record.
fn registry_state_filter(raw: Option<&str>) -> Result<(), String> {
    let trimmed = raw.unwrap_or_default().trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("paused") {
        return Ok(());
    }
    Err(format!("unknown state '{trimmed}', must be paused"))
}

impl ApiImpl {
    /// Lists the paused sandboxes the catalog records: one row per sandbox,
    /// its newest ready snapshot.
    pub(super) async fn list_registry_sandboxes(
        &self,
        query_params: &models::RegistrySandboxesGetQueryParams,
    ) -> Result<RegistrySandboxesGetResponse, ()> {
        if let Err(message) = registry_state_filter(query_params.state.as_deref()) {
            return Ok(RegistrySandboxesGetResponse::Status400_BadRequest(
                Self::error(400, message),
            ));
        }
        let rows = match self.list_paused_sandboxes().await {
            Ok(rows) => rows,
            Err(err) => {
                return Ok(
                    RegistrySandboxesGetResponse::Status503_TheRegistryCouldNotBeRead(Self::error(
                        503,
                        format!("snapshot catalog unavailable: {err:#}"),
                    )),
                );
            }
        };
        let node_filter = query_params.node_id.as_deref().unwrap_or_default().trim();
        let page_token =
            match parse_page_token(query_params.next_token.as_deref().unwrap_or_default()) {
                Ok(token) => token,
                Err(message) => {
                    return Ok(RegistrySandboxesGetResponse::Status400_BadRequest(
                        Self::error(400, message),
                    ));
                }
            };
        let mut matched: Vec<paused::PausedSandboxRow> = rows
            .into_iter()
            .filter(|row| {
                node_filter.is_empty() || row.origin_node_id.as_deref() == Some(node_filter)
            })
            .filter(|row| page_token.is_none_or(|after| row.sandbox_id > after))
            .collect();
        // Keyset paging needs a total order the backend does not promise.
        matched.sort_by_key(|row| row.sandbox_id);
        let now_unix_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_millis() as i64)
            .unwrap_or(0);
        let mut page = models::RegistrySandboxListing::new(Vec::new(), now_unix_ms);
        let page_size = query_params.limit.unwrap_or(0) as usize;
        if page_size > 0 && page_size < matched.len() {
            matched.truncate(page_size);
            page.next_token = Some(
                matched
                    .last()
                    .expect("truncate to a positive page_size leaves at least one row")
                    .sandbox_id
                    .to_string(),
            );
        }
        page.sandboxes = matched.iter().map(models::RegistrySandbox::from).collect();
        Ok(RegistrySandboxesGetResponse::Status200_SuccessfullyReturnedTheRegistryPage(page))
    }
}

#[cfg(test)]
mod registry_listing_tests {
    use std::sync::Arc;

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;
    use serde_json::{json, Value};
    use uuid::Uuid;

    use agentenv_http_server::apis::admin::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::{
        in_memory_snapshot_manager, mock_paused_sandbox_config, mock_snapshot_manager,
        paused_sandbox_record,
    };
    use crate::snapshot::{SnapshotManager, SnapshotRecord};
    use crate::types::SandboxId;
    use aenv_node::orchestrator::Orchestrator;

    fn sandbox_id(nth: u8) -> SandboxId {
        SandboxId::from_uuid(
            Uuid::parse_str(&format!("0192b000-0000-7000-8000-0000000000{nth:02x}"))
                .expect("a uuid"),
        )
    }

    fn row(nth: u8, node: &str) -> SnapshotRecord {
        paused_sandbox_record(
            sandbox_id(nth),
            Some(node),
            mock_paused_sandbox_config(),
            1_700_000_001_000 + i64::from(nth),
        )
    }

    async fn api_over(snapshot_manager: SnapshotManager) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::new(snapshot_manager),
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    async fn api_holding(rows: Vec<SnapshotRecord>) -> Arc<ApiImpl> {
        let (snapshot_manager, catalog) = in_memory_snapshot_manager();
        for record in rows {
            catalog.seed(record);
        }
        api_over(snapshot_manager).await
    }

    fn params() -> models::RegistrySandboxesGetQueryParams {
        models::RegistrySandboxesGetQueryParams {
            state: None,
            node_id: None,
            limit: None,
            next_token: None,
        }
    }

    async fn list(
        api: &ApiImpl,
        query_params: models::RegistrySandboxesGetQueryParams,
    ) -> RegistrySandboxesGetResponse {
        api.registry_sandboxes_get(
            &Method::GET,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &query_params,
        )
        .await
        .expect("the handler answers")
    }

    fn body(response: RegistrySandboxesGetResponse) -> Value {
        match response {
            RegistrySandboxesGetResponse::Status200_SuccessfullyReturnedTheRegistryPage(page) => {
                serde_json::to_value(page).expect("the page serializes")
            }
            other => panic!("expected a page, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_page_carries_every_field_the_gateway_renders() {
        let record = row(1, "node-a");
        let api = api_holding(vec![record.clone()]).await;

        let page = body(list(&api, params()).await);

        let sandboxes = page["sandboxes"].as_array().expect("rows");
        assert_eq!(sandboxes.len(), 1);
        assert_eq!(
            sandboxes[0],
            json!({
                "sandboxID": sandbox_id(1).to_string(),
                "clusterID": "",
                "state": "paused",
                "generation": 0,
                "originNodeID": "node-a",
                "claimedByNodeID": "",
                "snapshotID": record.id.to_string(),
                "holderNodeID": "node-a",
                "pausedAtUnixMs": record.created_at_unix_ms,
                "updatedAtUnixMs": record.created_at_unix_ms,
                "leaseExpiresAtUnixMs": Value::Null,
                "sandboxExpiresAtUnixMs": Value::Null,
                "executionID": "",
            }),
            "the REST page is the gateway's registrySandboxItem field for field; a \
             paused sandbox holds no lease, no deadline and no incarnation"
        );
        assert!(
            page["databaseTimeUnixMs"].as_i64().expect("a clock") > 0,
            "got {page}"
        );
    }

    #[tokio::test]
    async fn only_the_newest_pause_of_a_sandbox_is_listed() {
        let older = paused_sandbox_record(
            sandbox_id(1),
            Some("node-a"),
            mock_paused_sandbox_config(),
            1_700_000_001_000,
        );
        let newer = paused_sandbox_record(
            sandbox_id(1),
            Some("node-b"),
            mock_paused_sandbox_config(),
            1_700_000_002_000,
        );
        let api = api_holding(vec![older, newer.clone()]).await;

        let page = body(list(&api, params()).await);

        let sandboxes = page["sandboxes"].as_array().expect("rows");
        assert_eq!(sandboxes.len(), 1, "one sandbox, one row: got {page}");
        assert_eq!(sandboxes[0]["snapshotID"], json!(newer.id.to_string()));
        assert_eq!(sandboxes[0]["holderNodeID"], json!("node-b"));
    }

    #[tokio::test]
    async fn a_checkpoint_taken_while_running_is_not_a_paused_sandbox() {
        let mut checkpoint = row(1, "node-a");
        checkpoint
            .committed
            .as_mut()
            .expect("a committed row")
            .paused_sandbox = None;
        let api = api_holding(vec![checkpoint, row(2, "node-a")]).await;

        let page = body(list(&api, params()).await);

        let sandboxes = page["sandboxes"].as_array().expect("rows");
        assert_eq!(sandboxes.len(), 1, "got {page}");
        assert_eq!(sandboxes[0]["sandboxID"], json!(sandbox_id(2).to_string()));
    }

    #[tokio::test]
    async fn the_last_page_omits_the_next_token() {
        let api = api_holding(vec![row(1, "node-a")]).await;

        let page = body(list(&api, params()).await);

        assert!(
            !page
                .as_object()
                .expect("an object")
                .contains_key("nextToken"),
            "got {page}"
        );
    }

    #[tokio::test]
    async fn a_limit_pages_by_sandbox_id() {
        let api = api_holding(vec![row(3, "node-a"), row(1, "node-a"), row(2, "node-a")]).await;

        let first = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    limit: Some(2),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(
            first["sandboxes"][0]["sandboxID"],
            json!(sandbox_id(1).to_string())
        );
        assert_eq!(
            first["sandboxes"][1]["sandboxID"],
            json!(sandbox_id(2).to_string())
        );
        assert_eq!(first["nextToken"], json!(sandbox_id(2).to_string()));

        let second = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    limit: Some(2),
                    next_token: first["nextToken"].as_str().map(str::to_string),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(second["sandboxes"].as_array().expect("rows").len(), 1);
        assert_eq!(
            second["sandboxes"][0]["sandboxID"],
            json!(sandbox_id(3).to_string())
        );
    }

    #[tokio::test]
    async fn the_node_filter_selects_by_the_node_that_holds_the_bytes() {
        let api = api_holding(vec![row(1, "node-a"), row(2, "node-z")]).await;

        let by_node = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    node_id: Some("node-z".to_string()),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(by_node["sandboxes"].as_array().expect("rows").len(), 1);
        assert_eq!(by_node["sandboxes"][0]["holderNodeID"], json!("node-z"));

        let by_state = body(
            list(
                &api,
                models::RegistrySandboxesGetQueryParams {
                    state: Some("paused".to_string()),
                    ..params()
                },
            )
            .await,
        );
        assert_eq!(
            by_state["sandboxes"].as_array().expect("rows").len(),
            2,
            "paused is the only state a row can be in, so the filter selects everything"
        );
    }

    #[tokio::test]
    async fn an_unknown_state_is_refused_rather_than_ignored() {
        let api = api_holding(vec![row(1, "node-a")]).await;

        let response = list(
            &api,
            models::RegistrySandboxesGetQueryParams {
                state: Some("running".to_string()),
                ..params()
            },
        )
        .await;

        assert!(
            matches!(
                response,
                RegistrySandboxesGetResponse::Status400_BadRequest(_)
            ),
            "a filter that silently does not apply returns every row with a 200, got \
             {response:?}"
        );
    }

    #[tokio::test]
    async fn an_unreadable_catalog_answers_503() {
        let api = api_over(mock_snapshot_manager()).await;

        let response = list(&api, params()).await;

        assert!(
            matches!(
                response,
                RegistrySandboxesGetResponse::Status503_TheRegistryCouldNotBeRead(_)
            ),
            "got {response:?}"
        );
    }

    #[test]
    fn the_query_parameters_stay_a_closed_set() {
        // Exhaustive destructuring: a fifth parameter stops compiling here
        // before it can reach the spec.
        let models::RegistrySandboxesGetQueryParams {
            state,
            node_id,
            limit,
            next_token,
        } = params();
        assert!(
            state.is_none() && node_id.is_none() && limit.is_none() && next_token.is_none(),
            "the default page asks for nothing"
        );
    }
}

#[cfg(test)]
mod registry_cursor_tests {
    use super::parse_page_token;
    use crate::types::SandboxId;

    #[test]
    fn an_empty_or_blank_next_token_means_the_first_page() {
        assert_eq!(parse_page_token("").expect("first page"), None);
        assert_eq!(parse_page_token("   ").expect("first page"), None);
    }

    #[test]
    fn a_sandbox_id_next_token_is_accepted() {
        let id = SandboxId::new();
        assert_eq!(
            parse_page_token(&id.to_string()).expect("a valid cursor"),
            Some(id)
        );
    }

    #[test]
    fn a_token_that_is_not_a_sandbox_id_is_refused_not_an_empty_page() {
        assert!(parse_page_token("zzzz").is_err());
    }
}
