use crate::{Result, Api};
use k8s_openapi::api::certificates::v1::CertificateSigningRequest;
use kube_core::params::{Patch, PatchParams};

impl Api<CertificateSigningRequest> {
    /// Partially update approval of the specified CertificateSigningRequest.
    pub async fn patch_approval<P: serde::Serialize + std::fmt::Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<CertificateSigningRequest> {
        self.patch_subresource("approval", name, pp, patch).await
    }

    /// Get the CertificateSigningRequest. May differ from get(name)
    pub async fn get_approval(&self, name: &str) -> Result<CertificateSigningRequest> {
        self.get_subresource("approval", name).await
    }
}
