use crate::{
    Error, Result, Api,
};
use k8s_openapi::api::{
    authentication::v1::TokenRequest,
    core::v1::{Node, ServiceAccount},
};
use kube_core::{params::{Patch, PatchParams, PostParams}, Resource};
use serde::de::DeserializeOwned;

mod csr;

impl<K> Api<K>
where
    K: kube_core::util::Restart + Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    /// Trigger a restart of a Resource.
    pub async fn restart(&self, name: &str) -> Result<K> {
        let annotation = format!(
            "{{\"metadata\":{{\"annotations\":{{\"kubectl.kubernetes.io/restartedAt\":\"{}\"}}}}}}",
            jiff::Timestamp::now()
        );
        let patch = Patch::Merge(serde_json::from_str::<serde_json::Value>(&annotation).map_err(Error::SerdeError)?);
        self.patch(name, &PatchParams::default(), &patch).await
    }
}

impl Api<Node> {
    /// Cordon a Node.
    pub async fn cordon(&self, name: &str) -> Result<Node> {
        let patch = Patch::Merge(serde_json::json!({
            "spec": {
                "unschedulable": true
            }
        }));
        self.patch(name, &PatchParams::default(), &patch).await
    }

    /// Uncordon a Node.
    pub async fn uncordon(&self, name: &str) -> Result<Node> {
        let patch = Patch::Merge(serde_json::json!({
            "spec": {
                "unschedulable": false
            }
        }));
        self.patch(name, &PatchParams::default(), &patch).await
    }
}

impl Api<ServiceAccount> {
    /// Create a TokenRequest of a ServiceAccount
    pub async fn create_token_request(
        &self,
        name: &str,
        pp: &PostParams,
        token_request: &TokenRequest,
    ) -> Result<TokenRequest> {
        self.create_subresource("token", name, pp, token_request).await
    }
}
