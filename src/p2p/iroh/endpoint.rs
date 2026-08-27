use anyhow::Context;
use iroh::EndpointAddr;

use super::IROH_BACKEND_ID;
use crate::p2p::error::{Error, Result};
use crate::p2p::types::P2pEndpoint;

impl P2pEndpoint {
    pub fn from_iroh_addr(addr: &EndpointAddr) -> Result<Self> {
        Ok(Self {
            backend: IROH_BACKEND_ID.to_string(),
            address: serde_json::to_string(addr).context("serialize iroh endpoint address")?,
        })
    }

    pub fn to_iroh_addr(&self) -> Result<EndpointAddr> {
        if self.backend != IROH_BACKEND_ID {
            return Err(Error::InvalidDescriptor {
                reason: format!("unsupported P2P endpoint backend {}", self.backend),
            });
        }
        Ok(serde_json::from_str(&self.address).context("parse iroh endpoint address")?)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use iroh::{EndpointAddr, SecretKey};

    use super::*;

    #[test]
    fn iroh_endpoint_round_trips_endpoint_addr() {
        let addr = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr("127.0.0.1:12345".parse::<SocketAddr>().expect("addr"));

        let endpoint = P2pEndpoint::from_iroh_addr(&addr).expect("encode endpoint");
        let decoded = endpoint.to_iroh_addr().expect("decode endpoint");

        assert_eq!(endpoint.backend, IROH_BACKEND_ID);
        assert_eq!(decoded, addr);
    }

    #[test]
    fn iroh_endpoint_rejects_non_iroh_backend() {
        let endpoint = P2pEndpoint {
            backend: "other".to_string(),
            address: "{}".to_string(),
        };

        let err = endpoint.to_iroh_addr().expect_err("decode should fail");

        assert!(matches!(err, Error::InvalidDescriptor { .. }));
    }

    #[test]
    fn iroh_endpoint_rejects_invalid_address_json() {
        let endpoint = P2pEndpoint {
            backend: IROH_BACKEND_ID.to_string(),
            address: "not json".to_string(),
        };

        let err = endpoint.to_iroh_addr().expect_err("decode should fail");

        assert!(matches!(err, Error::Internal(_)));
    }
}
