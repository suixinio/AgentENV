//! Redis binding-store backend compatible with the gateway routing reader.

pub mod scripts;

#[cfg(test)]
pub mod harness;
#[cfg(test)]
mod tests;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use super::arbitration::BindingDecision;
use super::record::{
    binding_key, marshal_record, node_index_key, parse_record, DEFAULT_KEY_PREFIX,
};
use super::{Binding, BindingDeleteOutcome, BindingStore, BindingStoreError, BindingStoreSettings};
use crate::node_registry::types::{Node, RosterEntry};

/// Redis-specific binding-store configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisBindingStoreConfig {
    /// `redis://host:port[/db]`.
    pub url: String,
    /// Gateway-compatible key namespace.
    pub key_prefix: String,
    /// Reverse-index TTL, independent from projection TTL.
    pub node_index_ttl: Duration,
    /// Per-command response timeout.
    /// Applied to every Redis command before reconnecting.
    pub response_timeout: Duration,
    /// Fresh connection-attempt timeout.
    pub connect_timeout: Duration,
}

impl Default for RedisBindingStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            node_index_ttl: Duration::from_secs(3600),
            response_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_millis(3000),
        }
    }
}

fn backend(err: impl std::fmt::Display) -> BindingStoreError {
    BindingStoreError::new(err.to_string())
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

pub struct RedisBindingStore {
    connection: redis::aio::ConnectionManager,
    redis_config: RedisBindingStoreConfig,
    settings: BindingStoreSettings,
}

impl RedisBindingStore {
    pub async fn connect(
        redis_config: RedisBindingStoreConfig,
        settings: BindingStoreSettings,
    ) -> Result<Self, BindingStoreError> {
        let client = redis::Client::open(redis_config.url.as_str()).map_err(backend)?;
        let manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(redis_config.response_timeout))
            .set_connection_timeout(Some(redis_config.connect_timeout));
        let connection = redis::aio::ConnectionManager::new_with_config(client, manager_config)
            .await
            .map_err(backend)?;
        Ok(Self {
            connection,
            redis_config,
            settings,
        })
    }

    fn binding_key(&self, sandbox_id: &str) -> String {
        binding_key(&self.redis_config.key_prefix, sandbox_id)
    }

    fn node_index_key(&self, node_id: &str) -> String {
        node_index_key(&self.redis_config.key_prefix, node_id)
    }

    fn decision_from_label(label: &str) -> BindingDecision {
        match label {
            "installed" => BindingDecision::Installed,
            "installed_unknown" => BindingDecision::InstalledUnknown,
            "refreshed" => BindingDecision::Refreshed,
            "superseded" => BindingDecision::Superseded,
            "rejected_older" => BindingDecision::RejectedOlder,
            "rejected_unknown" => BindingDecision::RejectedUnknown,
            _ => BindingDecision::NotArbitrated,
        }
    }

    /// Raw test connection for assertions outside the store API.
    #[cfg(test)]
    pub fn raw_connection(&self) -> redis::aio::ConnectionManager {
        self.connection.clone()
    }

    #[cfg(test)]
    pub fn config(&self) -> &RedisBindingStoreConfig {
        &self.redis_config
    }
}

#[async_trait]
impl BindingStore for RedisBindingStore {
    async fn get(
        &self,
        sandbox_id: &str,
        _now: SystemTime,
    ) -> Result<Option<Binding>, BindingStoreError> {
        let mut connection = self.connection.clone();
        let raw: Option<Vec<u8>> = connection
            .get(self.binding_key(sandbox_id))
            .await
            .map_err(backend)?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let Some(record) = parse_record(&raw) else {
            return Ok(None);
        };
        Ok(Some(Binding {
            node: record.node.into(),
            execution_id: record.execution_id,
            projection_ttl: Duration::ZERO,
        }))
    }

    async fn record(
        &self,
        sandbox_id: &str,
        binding: Binding,
        _now: SystemTime,
    ) -> Result<BindingDecision, BindingStoreError> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Ok(BindingDecision::NotArbitrated);
        }
        let value = marshal_record(&binding.node, &binding.execution_id);
        let mut connection = self.connection.clone();
        let result: Vec<(String, String)> = scripts::record_script()
            .key(self.binding_key(sandbox_id))
            .key(self.node_index_key(&binding.node.id))
            .arg(value)
            .arg(&binding.node.id)
            .arg(sandbox_id)
            .arg(ms(self.settings.binding_ttl))
            .arg(&self.redis_config.key_prefix)
            .arg(ms(self.redis_config.node_index_ttl))
            .arg(&binding.execution_id)
            .arg(ms(binding.projection_ttl))
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        let (_, decision) = result.into_iter().next().ok_or_else(|| {
            BindingStoreError::new("record script returned no decision".to_string())
        })?;
        Ok(Self::decision_from_label(&decision))
    }

    async fn reconcile_node(
        &self,
        node: Node,
        roster: Vec<RosterEntry>,
        _now: SystemTime,
    ) -> Result<Vec<(String, BindingDecision)>, BindingStoreError> {
        use std::collections::HashMap;

        let mut normalized: HashMap<String, RosterEntry> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for entry in roster {
            let sandbox_id = entry.sandbox_id.trim().to_string();
            if sandbox_id.is_empty() {
                continue;
            }
            if !normalized.contains_key(&sandbox_id) {
                order.push(sandbox_id.clone());
            }
            normalized.insert(
                sandbox_id.clone(),
                RosterEntry {
                    sandbox_id,
                    ..entry
                },
            );
        }

        let node_json = serde_json::to_string(&super::record::WireNode::from(&node))
            .expect("WireNode serialization is infallible for this shape");

        let sandbox_ids: Vec<String> = order.clone();
        let execution_ids: Vec<String> = order
            .iter()
            .map(|id| normalized[id].execution_id.clone())
            .collect();
        let projection_ttls: Vec<String> = order
            .iter()
            .map(|id| ms(normalized[id].projection_ttl).to_string())
            .collect();

        let mut connection = self.connection.clone();
        let script = scripts::reconcile_script(self.settings.projection_authoritative);
        let mut invocation = script.key(self.node_index_key(&node.id));
        invocation
            .arg(&node.id)
            .arg(&node_json)
            .arg(ms(self.settings.binding_ttl))
            .arg(&self.redis_config.key_prefix)
            .arg(ms(self.redis_config.node_index_ttl))
            .arg(order.len());
        for id in &sandbox_ids {
            invocation.arg(id);
        }
        for id in &execution_ids {
            invocation.arg(id);
        }
        for ttl in &projection_ttls {
            invocation.arg(ttl);
        }

        let result: Vec<(String, String)> = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        Ok(result
            .into_iter()
            .map(|(id, decision)| (id, Self::decision_from_label(&decision)))
            .collect())
    }

    async fn delete(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        _now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError> {
        let sandbox_id = sandbox_id.trim();
        let execution_id = execution_id.trim();
        if sandbox_id.is_empty() || execution_id.is_empty() {
            return Ok(BindingDeleteOutcome::Absent);
        }
        let mut connection = self.connection.clone();
        let outcome: String = scripts::delete_script()
            .key(self.binding_key(sandbox_id))
            .arg(sandbox_id)
            .arg(execution_id)
            .arg(&self.redis_config.key_prefix)
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        Ok(match outcome.as_str() {
            "deleted" => BindingDeleteOutcome::Deleted,
            "deleted_unknown_incumbent" => BindingDeleteOutcome::DeletedUnknownIncumbent,
            "rejected_stale" => BindingDeleteOutcome::RejectedStale,
            _ => BindingDeleteOutcome::Absent,
        })
    }
}
