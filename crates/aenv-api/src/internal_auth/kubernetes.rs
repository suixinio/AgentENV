//! Resolving a projected ServiceAccount token to the node its Pod runs on.
//!
//! Two calls, and both are needed. `TokenReview` says the token is genuine and
//! which ServiceAccount it belongs to — but a ServiceAccount is shared by
//! every Pod of the DaemonSet, so it names no machine. The `pod-name` extra
//! Kubernetes attaches to a bound token names the Pod, and reading that Pod's
//! `spec.nodeName` is what turns the token into a machine.

use async_trait::async_trait;
use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, PostParams};
use kube::Client;
use tracing::warn;

use super::{CallerError, CallerNode, INTERNAL_AUDIENCE};

/// The extra Kubernetes attaches to a token bound to a Pod.
const POD_NAME_EXTRA: &str = "authentication.kubernetes.io/pod-name";

pub struct KubernetesCallerNode {
    client: Client,
    namespace: String,
}

impl KubernetesCallerNode {
    pub fn new(client: Client, namespace: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl CallerNode for KubernetesCallerNode {
    async fn node_of(&self, token: &str) -> Result<String, CallerError> {
        let review = TokenReview {
            spec: TokenReviewSpec {
                token: Some(token.to_string()),
                audiences: Some(vec![INTERNAL_AUDIENCE.to_string()]),
            },
            ..Default::default()
        };
        let reviewed = Api::<TokenReview>::all(self.client.clone())
            .create(&PostParams::default(), &review)
            .await
            .map_err(|err| CallerError::Unavailable(format!("TokenReview: {err}")))?;

        let status = reviewed.status.ok_or_else(|| {
            CallerError::Unavailable("TokenReview answered without a status".to_string())
        })?;
        if status.authenticated != Some(true) {
            return Err(CallerError::Unauthenticated);
        }
        // A token minted for another audience authenticates against the
        // Kubernetes API and must not authenticate here.
        if !status
            .audiences
            .as_ref()
            .is_some_and(|audiences| audiences.iter().any(|a| a == INTERNAL_AUDIENCE))
        {
            return Err(CallerError::Unauthenticated);
        }

        let user = status.user.ok_or(CallerError::Unauthenticated)?;
        let pod_name = user
            .extra
            .as_ref()
            .and_then(|extra| extra.get(POD_NAME_EXTRA))
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| {
                // A ServiceAccount token that is not bound to a Pod names no
                // machine, so there is nothing to scope its questions to.
                warn!(
                    "an internal call presented a token with no {POD_NAME_EXTRA}; only a \
                     projected token bound to a Pod can be scoped to a node"
                );
                CallerError::Unauthenticated
            })?;

        let pod = Api::<Pod>::namespaced(self.client.clone(), &self.namespace)
            .get(&pod_name)
            .await
            .map_err(|err| CallerError::Unavailable(format!("read pod {pod_name}: {err}")))?;
        pod.spec
            .and_then(|spec| spec.node_name)
            .ok_or_else(|| CallerError::Unavailable(format!("pod {pod_name} names no node")))
    }
}
