use std::path::PathBuf;
use std::time::Duration;

use crate::cfg::P2pConfig;

pub use crate::cfg::P2pTransportKind;

/// The backend id a transport kind advertises to peers.
///
/// 🔴 Not an inherent method on [`P2pTransportKind`]: that type is a *config*
/// value — `[p2p].transport` — and lives with the rest of the config, which
/// knows nothing about which transport implementations are linked into this
/// process. The mapping from the configured name to a linked backend belongs
/// to the half that links them.
pub fn backend_id(kind: P2pTransportKind) -> Option<&'static str> {
    match kind {
        P2pTransportKind::Disabled => None,
        P2pTransportKind::Iroh => Some(super::iroh::IROH_BACKEND_ID),
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedP2pConfig {
    pub transport: P2pTransportKind,
    pub store_dir: PathBuf,
    pub listen_addr: Option<String>,
    pub lookup_timeout: Duration,
    pub fetch_timeout: Duration,
    pub peer_discovery_refresh_interval: Duration,
}

impl ResolvedP2pConfig {
    pub fn from_config(p2p: &P2pConfig) -> Self {
        let transport = if p2p.enabled {
            p2p.transport
        } else {
            P2pTransportKind::Disabled
        };

        Self {
            transport,
            store_dir: p2p.store_dir.clone(),
            listen_addr: Some(str::trim(p2p.listen_addr.as_str()))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            lookup_timeout: Duration::from_millis(p2p.lookup_timeout_ms),
            fetch_timeout: Duration::from_millis(p2p.fetch_timeout_ms),
            peer_discovery_refresh_interval: Duration::from_secs(
                p2p.peer_discovery_refresh_interval_secs,
            )
            .max(Duration::from_secs(1)),
        }
    }
}
