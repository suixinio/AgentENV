use std::collections::HashMap;
use std::time::SystemTime;

use crate::orchestrator::{SandboxListFilter, SandboxState};
use crate::types::SandboxId;
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use super::super::pagination::PaginationCursor;
use super::super::paused;
use super::ApiImpl;

fn parse_metadata_filter(raw: &Option<String>) -> Option<HashMap<String, String>> {
    let raw = raw.as_ref()?;
    let map: HashMap<String, String> = url::form_urlencoded::parse(raw.as_bytes())
        .filter(|(key, value)| !key.is_empty() && !value.is_empty())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

impl ApiImpl {
    pub(super) async fn list_get(
        &self,
        query_params: &models::SandboxesGetQueryParams,
    ) -> Result<SandboxesGetResponse, ()> {
        let filter = SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: parse_metadata_filter(&query_params.metadata),
        };

        let list = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };

        let out = list.into_iter().map(models::ListedSandbox::from).collect();

        Ok(SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes(out))
    }

    pub(super) async fn list_v2_get(
        &self,
        query_params: &models::V2SandboxesGetQueryParams,
    ) -> Result<V2SandboxesGetResponse, ()> {
        // Only two states are supported. With both, or neither, named, the
        // listing spans running records and paused rows alike.
        let (want_running, want_paused) = if query_params.state.len() == 1 {
            match query_params.state[0] {
                models::SandboxState::Running => (true, false),
                models::SandboxState::Paused => (false, true),
            }
        } else {
            (true, true)
        };
        let user_metadata = parse_metadata_filter(&query_params.metadata);
        let filter = SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: user_metadata.clone(),
        };

        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match PaginationCursor::parse(token) {
                Ok(cursor) => cursor,
                Err(err) => {
                    return Ok(V2SandboxesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {}", err),
                    )));
                }
            },
            None => PaginationCursor::new(SystemTime::now(), SandboxId::max()),
        };

        // Running records are read either way: a resumed sandbox keeps the row
        // it was resumed from, and only its record says it is not paused.
        let running = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(V2SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };
        let mut listed: Vec<(SystemTime, SandboxId, models::ListedSandbox)> = if want_running {
            running
                .iter()
                .map(|sandbox| {
                    (
                        sandbox.created_at,
                        sandbox.id,
                        models::ListedSandbox::from(sandbox.clone()),
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        if want_paused {
            let running_ids: std::collections::HashSet<SandboxId> =
                running.iter().map(|sandbox| sandbox.id).collect();
            let paused = match self.list_paused_snapshots(user_metadata).await {
                Ok(paused) => paused,
                Err(err) => {
                    return Ok(V2SandboxesGetResponse::Status500_ServerError(
                        Self::snapshot_manager_error(&err),
                    ));
                }
            };
            for record in &paused {
                let Some(sandbox_id) = paused::paused_sandbox_id(record) else {
                    continue;
                };
                // A sandbox resumed since its last pause is listed once, as running.
                if running_ids.contains(&sandbox_id) {
                    continue;
                }
                let Some(model) = paused::listed_paused_sandbox(record) else {
                    continue;
                };
                listed.push((paused::paused_started_at(record), sandbox_id, model));
            }
        }

        let page = cursor.paginate(
            listed,
            query_params.limit,
            |a, b| PaginationCursor::compare_desc(a.0, &a.1, b.0, &b.1),
            |entry, cursor| {
                PaginationCursor::compare_desc(entry.0, &entry.1, cursor.time(), cursor.value())
            },
            |entry| PaginationCursor::new(entry.0, entry.1),
        );

        let out = page
            .items
            .into_iter()
            .map(|(_, _, model)| model)
            .collect::<Vec<_>>();

        Ok(
            V2SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes {
                body: out,
                x_next_token: page.next_token,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_metadata_filter_with_none_returns_none() {
        assert_eq!(parse_metadata_filter(&None), None);
    }

    #[test]
    fn parse_metadata_filter_with_empty_string_returns_none() {
        assert_eq!(parse_metadata_filter(&Some(String::new())), None);
    }

    #[test]
    fn parse_metadata_filter_with_single_pair() {
        let result = parse_metadata_filter(&Some("key=value".to_string()));
        assert_eq!(
            result,
            Some(HashMap::from([("key".to_string(), "value".to_string())]))
        );
    }

    #[test]
    fn parse_metadata_filter_with_multiple_pairs() {
        let result = parse_metadata_filter(&Some("a=1&b=2".to_string()));
        assert_eq!(result.map(|m| m.len()), Some(2));
    }

    #[test]
    fn parse_metadata_filter_filters_empty_keys() {
        let result = parse_metadata_filter(&Some("=value&key=val".to_string()));
        let map = result.unwrap();
        assert!(!map.contains_key(""));
        assert!(map.contains_key("key"));
    }

    #[test]
    fn parse_metadata_filter_with_encoded_characters() {
        let result = parse_metadata_filter(&Some(
            "key%20with%20spaces=value%20with%20spaces".to_string(),
        ));
        assert_eq!(
            result,
            Some(HashMap::from([(
                "key with spaces".to_string(),
                "value with spaces".to_string()
            )]))
        );
    }
}
