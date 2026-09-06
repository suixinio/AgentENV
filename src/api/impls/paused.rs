//! The paused sandboxes this API knows: the snapshot catalog's sandbox-source
//! rows, one per sandbox, its newest ready snapshot.
//!
//! A paused sandbox has no record anywhere else. Everything the REST surface
//! says about one, and everything a resume rebuilds it from, comes from here.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentenv_http_server::models;
use tracing::warn;

use super::ApiImpl;
use crate::orchestrator::{CreateSandboxRequest, NewTimeout, SandboxExpiry, SandboxLaunchSource};
use crate::snapshot::{
    CatalogReadScope, SnapshotId, SnapshotListFilter, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind,
};
use crate::types::SandboxId;

/// One paused sandbox as the catalog describes it.
#[derive(Clone, Debug)]
pub(in crate::api) struct PausedSandboxRow {
    pub sandbox_id: SandboxId,
    pub snapshot_id: SnapshotId,
    pub origin_node_id: Option<String>,
    pub paused_at_unix_ms: i64,
}

impl PausedSandboxRow {
    fn of(record: &SnapshotRecord) -> Option<Self> {
        let SnapshotSource::Sandbox { source_sandbox_id } = &record.source else {
            return None;
        };
        Some(Self {
            sandbox_id: SandboxId::parse_str(source_sandbox_id).ok()?,
            snapshot_id: record.id.clone(),
            origin_node_id: record.origin_node_id.clone(),
            paused_at_unix_ms: record.created_at_unix_ms,
        })
    }
}

/// The sandbox this row was paused from, if it names one.
pub(in crate::api) fn paused_sandbox_id(record: &SnapshotRecord) -> Option<SandboxId> {
    match &record.source {
        SnapshotSource::Sandbox { source_sandbox_id } => {
            SandboxId::parse_str(source_sandbox_id).ok()
        }
        SnapshotSource::Template { .. } => None,
    }
}

impl ApiImpl {
    /// The newest ready snapshot paused from `sandbox_id`, or `None` when the
    /// sandbox has never paused or every pause of it was deleted.
    ///
    /// Only rows carrying a pause configuration count: a checkpoint taken
    /// while the sandbox ran is a template of it, not a pause.
    pub(in crate::api) async fn latest_paused_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        let filter = SnapshotListFilter::sandbox_snapshots(Some(sandbox_id.to_string()), None);
        let mut cursor = None;
        loop {
            let page = self
                .snapshot_manager
                .list_page_scoped(
                    filter.clone().paginated(None, cursor.take()),
                    CatalogReadScope::Resolvable,
                )
                .await?;
            if let Some(record) = page
                .items
                .iter()
                .find(|record| record.paused_sandbox().is_some())
            {
                return Ok(Some(record.clone()));
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(None),
            }
        }
    }

    /// Every paused sandbox, newest snapshot per sandbox.
    pub(in crate::api) async fn list_paused_snapshots(
        &self,
    ) -> anyhow::Result<Vec<SnapshotRecord>> {
        let mut filter = SnapshotListFilter {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            ..SnapshotListFilter::default()
        };
        let mut newest: HashMap<SandboxId, SnapshotRecord> = HashMap::new();
        loop {
            let page = self
                .snapshot_manager
                .list_page_scoped(filter.clone(), CatalogReadScope::Resolvable)
                .await?;
            for record in page.items {
                if record.paused_sandbox().is_none() {
                    continue;
                }
                let Some(sandbox_id) = paused_sandbox_id(&record) else {
                    continue;
                };
                // Pages are newest first, so the first row seen per sandbox wins.
                newest.entry(sandbox_id).or_insert(record);
            }
            match page.next {
                Some(next) => filter.cursor = Some(next),
                None => break,
            }
        }
        let mut rows: Vec<SnapshotRecord> = newest.into_values().collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.created_at_unix_ms));
        Ok(rows)
    }

    pub(in crate::api) async fn list_paused_sandboxes(
        &self,
    ) -> anyhow::Result<Vec<PausedSandboxRow>> {
        Ok(self
            .list_paused_snapshots()
            .await?
            .iter()
            .filter_map(PausedSandboxRow::of)
            .collect())
    }

    /// Deletes every snapshot paused from `sandbox_id`. Returns how many rows
    /// went away; zero means the sandbox had no pause to forget.
    pub(in crate::api) async fn forget_paused_snapshots(
        &self,
        sandbox_id: SandboxId,
    ) -> anyhow::Result<usize> {
        let filter = SnapshotListFilter::sandbox_snapshots(Some(sandbox_id.to_string()), None);
        let mut deleted = 0usize;
        loop {
            let page = self
                .snapshot_manager
                .list_page_scoped(filter.clone(), CatalogReadScope::AnyStatus)
                .await?;
            let paused: Vec<SnapshotId> = page
                .items
                .iter()
                .filter(|record| record.paused_sandbox().is_some())
                .map(|record| record.id.clone())
                .collect();
            if paused.is_empty() {
                break;
            }
            for snapshot_id in paused {
                match self.snapshot_manager.delete(snapshot_id.to_string()).await {
                    Ok(()) => deleted += 1,
                    Err(err) => {
                        warn!(%sandbox_id, %snapshot_id, error = %format_args!("{err:#}"), "failed to delete a paused snapshot");
                        return Err(err);
                    }
                }
            }
            // Deleting shifts the page window; start over from the newest.
        }
        Ok(deleted)
    }

    /// The request that rebuilds a paused sandbox from its row, under its own
    /// id, with the configuration it paused with.
    pub(in crate::api) fn restore_request(
        record: SnapshotRecord,
        timeout: NewTimeout,
    ) -> anyhow::Result<CreateSandboxRequest> {
        let paused = record.paused_sandbox().cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "snapshot {} carries no pause configuration to rebuild a sandbox from",
                record.id
            )
        })?;
        let expiry = match timeout {
            NewTimeout::Set(duration) | NewTimeout::EnsureMinimum(duration) => {
                SandboxExpiry::After(duration)
            }
            NewTimeout::UseExisting => match paused.timeout() {
                Some(duration) => SandboxExpiry::After(duration),
                None => SandboxExpiry::NotKeptHere,
            },
            NewTimeout::None => SandboxExpiry::NotKeptHere,
        };
        Ok(CreateSandboxRequest {
            preferred_node_id: record.origin_node_id.clone(),
            source: SandboxLaunchSource::SnapshotRecord(Box::new(record)),
            expiry,
            timeout_action: paused.timeout_action,
            auto_resume: paused.auto_resume,
            user_metadata: paused.user_metadata.clone(),
            env_vars: None,
            network_policy: paused.network_policy.clone(),
            secure: paused.secure,
            // A sandbox its owner locked comes back locked, with the token its
            // clients already hold.
            traffic_access_token: paused.traffic_access_token.clone(),
            // The row's own params travel inside the record; nothing overrides them.
            custom_extension_params: None,
            control_plane_config: paused.control_plane_config.clone(),
            execution_id: None,
        })
    }
}

fn started_at(unix_ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_millis(unix_ms.max(0) as u64))
}

/// A paused sandbox never expires on its own; clients read the same distant
/// date a non-expiring running sandbox reports.
fn never_ends() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(
        UNIX_EPOCH + Duration::from_secs(60 * 60 * 24 * 365 * 100),
    )
}

/// Renders a paused sandbox's row the way the listing endpoints describe one.
pub(in crate::api) fn listed_paused_sandbox(
    record: &SnapshotRecord,
) -> Option<models::ListedSandbox> {
    let paused = record.paused_sandbox()?;
    let sandbox_id = paused_sandbox_id(record)?;
    Some(models::ListedSandbox {
        template_id: paused.template_id.clone(),
        alias: paused.template_alias.clone(),
        sandbox_id: sandbox_id.into(),
        client_id: String::new(),
        started_at: started_at(paused.created_at_unix_ms),
        end_at: never_ends(),
        cpu_count: record.resources.cpu_count,
        memory_mb: record.resources.memory_mib,
        disk_size_mb: record.resources.disk_size_mib,
        metadata: paused.user_metadata.clone(),
        state: models::SandboxState::Paused,
        envd_version: record
            .committed
            .as_ref()
            .map(|committed| committed.runtime_versions.envd_version.clone())
            .unwrap_or_default(),
        // No run is live; the next resume mints one.
        execution_id: None,
    })
}

/// Renders a paused sandbox's row the way `GET /sandboxes/{id}` describes one.
pub(in crate::api) fn paused_sandbox_detail(
    record: &SnapshotRecord,
) -> Option<models::SandboxDetail> {
    let paused = record.paused_sandbox()?;
    let sandbox_id = paused_sandbox_id(record)?;
    let network = paused
        .network_policy
        .has_explicit_egress_rules()
        .then(|| models::SandboxNetworkConfig::from(&paused.network_policy));
    Some(models::SandboxDetail {
        template_id: paused.template_id.clone(),
        alias: paused.template_alias.clone(),
        sandbox_id: sandbox_id.into(),
        client_id: String::new(),
        started_at: started_at(paused.created_at_unix_ms),
        end_at: never_ends(),
        envd_version: record
            .committed
            .as_ref()
            .map(|committed| committed.runtime_versions.envd_version.clone())
            .unwrap_or_default(),
        envd_access_token: None,
        allow_internet_access: Some(super::sandbox::allow_internet_access_from_base_policy(
            paused.network_policy.base_policy,
        )),
        domain: None,
        cpu_count: record.resources.cpu_count,
        memory_mb: record.resources.memory_mib,
        disk_size_mb: record.resources.disk_size_mib,
        metadata: paused.user_metadata.clone(),
        state: models::SandboxState::Paused,
        network,
        lifecycle: Some(models::SandboxLifecycle {
            auto_resume: paused.auto_resume,
            on_timeout: paused.timeout_action.into(),
        }),
        execution_id: None,
    })
}

/// Where the user-visible clock of a paused row sits, for list ordering.
pub(in crate::api) fn paused_started_at(record: &SnapshotRecord) -> SystemTime {
    record
        .paused_sandbox()
        .map(|paused| paused.created_at())
        .unwrap_or(UNIX_EPOCH)
}
