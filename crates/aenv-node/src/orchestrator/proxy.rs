use std::{collections::HashMap, net::Ipv4Addr, time::SystemTime};

use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyTarget {
    pub ip: Ipv4Addr,
    /// The token a client must present to reach a non-envd port of this
    /// sandbox. It rides with the route so the check costs the lookup the
    /// proxy already does, rather than a second read of the record.
    pub traffic_access_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyLookupResult {
    Ready(ProxyTarget),
    NotFound,
    Unavailable(SandboxState),
    RouteMissing,
}

#[derive(Clone, Debug)]
pub struct ProxyRoute {
    target: ProxyTarget,
    version: u64,
    updated_at: SystemTime,
    execution_id: ExecutionId,
}

#[derive(Debug, Default)]
pub struct ProxyRouteTable {
    routes: HashMap<SandboxId, ProxyRoute>,
}

impl ProxyTarget {
    /// An open sandbox: every port it serves is reachable by whoever can
    /// route to it.
    pub fn new(host_interaction_ip: Ipv4Addr) -> Self {
        Self {
            ip: host_interaction_ip,
            traffic_access_token: None,
        }
    }

    pub fn with_traffic_access_token(mut self, token: Option<String>) -> Self {
        self.traffic_access_token = token;
        self
    }
}

impl ProxyRoute {
    pub fn new(target: ProxyTarget, version: u64, execution_id: ExecutionId) -> Self {
        Self {
            target,
            version,
            updated_at: SystemTime::now(),
            execution_id,
        }
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn target(&self) -> &ProxyTarget {
        &self.target
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn updated_at(&self) -> SystemTime {
        self.updated_at
    }
}

impl ProxyRouteTable {
    pub fn upsert(
        &mut self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        version: u64,
        execution_id: ExecutionId,
    ) -> ProxyRoute {
        let route = ProxyRoute::new(target, version, execution_id);
        self.routes.insert(sandbox_id, route.clone());
        route
    }

    pub fn remove(&mut self, sandbox_id: &SandboxId) -> Option<ProxyRoute> {
        self.routes.remove(sandbox_id)
    }

    #[cfg(test)]
    pub fn proxy_target(&self, sandbox_id: &SandboxId) -> Option<ProxyTarget> {
        self.routes
            .get(sandbox_id)
            .map(|route| route.target().clone())
    }

    pub fn route(&self, sandbox_id: &SandboxId) -> Option<&ProxyRoute> {
        self.routes.get(sandbox_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_table_only_exposes_inserted_routes() {
        let sandbox_id = SandboxId::new();
        let target = ProxyTarget::new(Ipv4Addr::LOCALHOST);
        let mut table = ProxyRouteTable::default();

        table.upsert(sandbox_id, target.clone(), 1, ExecutionId::new());
        assert_eq!(table.proxy_target(&sandbox_id), Some(target.clone()));

        table.upsert(sandbox_id, target.clone(), 2, ExecutionId::new());
        assert_eq!(table.proxy_target(&sandbox_id), Some(target));
        assert_eq!(table.routes.get(&sandbox_id).unwrap().version(), 2);
    }

    #[test]
    fn proxy_table_remove_drops_route() {
        let sandbox_id = SandboxId::new();
        let mut table = ProxyRouteTable::default();

        table.upsert(
            sandbox_id,
            ProxyTarget::new(Ipv4Addr::LOCALHOST),
            3,
            ExecutionId::new(),
        );
        let _removed = table.remove(&sandbox_id).unwrap();

        assert!(!table.routes.contains_key(&sandbox_id));
        assert!(table.proxy_target(&sandbox_id).is_none());
    }
}
