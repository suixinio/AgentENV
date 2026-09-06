//! Who a caller at this half's internal endpoints is.
//!
//! The broker presents its own projected ServiceAccount token, minted for the
//! `aenv-api` audience. This half asks Kubernetes whose token it is, then
//! asks which machine that Pod runs on. The answer is a node id, and it is
//! what bounds every internal endpoint: a broker asks only about the sandboxes
//! bound to the machine it is on.
//!
//! Node identity comes from Kubernetes and never from the request body. A
//! caller that could name its own node would be no bound at all.

use std::sync::Arc;

use async_trait::async_trait;

mod kubernetes;

pub use kubernetes::KubernetesCallerNode;

/// The audience a broker's token must be minted for. A token the Kubernetes
/// API would accept for itself is refused here, and this one is refused there.
pub const INTERNAL_AUDIENCE: &str = "aenv-api";

#[derive(Debug, thiserror::Error)]
pub enum CallerError {
    /// The token is not one this half accepts. Never says why.
    #[error("unauthenticated")]
    Unauthenticated,
    /// Kubernetes could not be asked. Not the same answer as a refusal: a
    /// caller told "no" stops asking, and an outage must not look like one.
    #[error("the caller's identity could not be established: {0}")]
    Unavailable(String),
}

/// Resolves a presented token to the node its holder runs on.
#[async_trait]
pub trait CallerNode: Send + Sync {
    async fn node_of(&self, token: &str) -> Result<String, CallerError>;
}

/// Answers from a fixed table. For tests, and for the assembly that has no
/// Kubernetes to ask.
pub struct StaticCallerNode {
    tokens: std::collections::HashMap<String, String>,
}

impl StaticCallerNode {
    pub fn new<I, K, V>(tokens: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            tokens: tokens
                .into_iter()
                .map(|(token, node)| (token.into(), node.into()))
                .collect(),
        }
    }
}

#[async_trait]
impl CallerNode for StaticCallerNode {
    async fn node_of(&self, token: &str) -> Result<String, CallerError> {
        self.tokens
            .get(token)
            .cloned()
            .ok_or(CallerError::Unauthenticated)
    }
}

/// Refuses every caller. What a deployment with no Kubernetes to ask installs,
/// so an internal endpoint answers "no" rather than "anyone".
pub struct NoCallerNode;

#[async_trait]
impl CallerNode for NoCallerNode {
    async fn node_of(&self, _token: &str) -> Result<String, CallerError> {
        Err(CallerError::Unauthenticated)
    }
}

/// The shared handle every internal route holds.
pub type SharedCallerNode = Arc<dyn CallerNode>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_known_token_resolves_to_its_node_and_an_unknown_one_is_refused() {
        let caller = StaticCallerNode::new([("token-a", "node-a"), ("token-b", "node-b")]);

        assert_eq!(caller.node_of("token-a").await.unwrap(), "node-a");
        assert_eq!(caller.node_of("token-b").await.unwrap(), "node-b");
        assert!(matches!(
            caller.node_of("token-c").await,
            Err(CallerError::Unauthenticated)
        ));
    }

    #[tokio::test]
    async fn the_refusing_caller_answers_no_to_everything() {
        assert!(matches!(
            NoCallerNode.node_of("anything").await,
            Err(CallerError::Unauthenticated)
        ));
    }
}
