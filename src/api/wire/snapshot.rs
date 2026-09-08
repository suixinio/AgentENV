use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;
use chrono::TimeZone;

use crate::snapshot::{SnapshotRecord, SnapshotSource, TemplateBuildStatus};

use super::system_time_from_unix_ms;

impl From<SnapshotRecord> for models::SnapshotInfo {
    fn from(record: SnapshotRecord) -> Self {
        let snapshot_id = record.id.to_string();
        let image_ref = record.published_rootfs_image_ref().map(str::to_owned);
        let names = if let Some(alias) = record.alias {
            vec![alias.to_string()]
        } else {
            vec![]
        };
        models::SnapshotInfo {
            snapshot_id,
            names,
            cpu_count: record.resources.cpu_count,
            memory_mb: record.resources.memory_mib,
            disk_size_mb: record.resources.disk_size_mib,
            created_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.created_at_unix_ms,
            )),
            updated_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.updated_at_unix_ms,
            )),
            image_ref,
        }
    }
}

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

pub fn datetime_from_unix_ms(unix_ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .timestamp_millis_opt(unix_ms)
        .single()
        .unwrap_or_else(chrono::Utc::now)
}

pub fn build_record_names(record: &SnapshotRecord) -> Vec<String> {
    record
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_string()])
        .unwrap_or_else(|| vec![record.id.to_string()])
}

pub fn build_record_envd_version(record: &SnapshotRecord) -> String {
    record
        .committed
        .as_ref()
        .map(|snapshot| snapshot.runtime_versions.envd_version.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn template_build_status(record: &SnapshotRecord) -> TemplateBuildStatus {
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
