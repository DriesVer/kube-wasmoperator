use futures::AsyncBufRead;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use crate::wasi_api::{Patch, PatchParams, PostParams};
use crate::{Api, Error, Result, wit_api};

use kube_core::response::Status;
pub use kube_core::subresource::{EvictParams, LogParams};

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub use kube_core::subresource::AttachParams;

pub use k8s_openapi::api::autoscaling::v1::{Scale, ScaleSpec, ScaleStatus};

#[cfg(feature = "ws")]
use crate::wasi_api::portforward::Portforwarder;
#[cfg(feature = "ws")]
use crate::wasi_api::remote_command::AttachedProcess;

/// Methods for [scale subresource](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#scale-subresource).
impl<K> Api<K>
where
    K: Clone + DeserializeOwned,
{
    /// Fetch the scale subresource
    pub async fn get_scale(&self, name: &str) -> Result<Scale> {
        let result = wit_api::get_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            None,
            wit_api::Scope::Subresource("scale".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Update the scale subresource
    pub async fn patch_scale<P: serde::Serialize + Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<Scale> {
        let patch_str = match patch {
            Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
        };

        let result = wit_api::patch_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            self.get_wit_patch_type(patch),
            patch_str,
            self.convert_patch_params(pp),
            wit_api::Scope::Subresource("scale".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Replace the scale subresource
    pub async fn replace_scale(&self, name: &str, pp: &PostParams, data: &Scale) -> Result<Scale> {
        let result = wit_api::replace_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            self.convert_post_params(pp),
            wit_api::Scope::Subresource("scale".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

/// Arbitrary subresources
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Debug,
{
    /// Display one or many sub-resources.
    pub async fn get_subresource(&self, subresource_name: &str, name: &str) -> Result<K> {
        let result = wit_api::get_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            None,
            wit_api::Scope::Subresource(subresource_name.to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Create an instance of the subresource
    pub async fn create_subresource<I, T>(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PostParams,
        data: &I,
    ) -> Result<T>
    where
        I: Serialize,
        T: DeserializeOwned,
    {
        let result = wit_api::create_subresource(
            self.get_wit_api_resource(),
            name.to_string(),
            subresource_name.to_string(),
            serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            self.convert_post_params(pp),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Patch an instance of the subresource
    pub async fn patch_subresource<P: serde::Serialize + Debug>(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let patch_str = match patch {
            Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
        };

        let result = wit_api::patch_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            self.get_wit_patch_type(patch),
            patch_str,
            self.convert_patch_params(pp),
            wit_api::Scope::Subresource(subresource_name.to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Replace an instance of the subresource
    pub async fn replace_subresource<I>(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PostParams,
        data: &I,
    ) -> Result<K>
    where
        I: Serialize,
    {
        let result = wit_api::replace_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            self.convert_post_params(pp),
            wit_api::Scope::Subresource(subresource_name.to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

// ----------------------------------------------------------------------------
// Ephemeral containers
// ----------------------------------------------------------------------------

/// Marker trait for objects that support the ephemeral containers sub resource.
///
/// See [`Api::get_ephemeral_containers`] et al.
pub trait Ephemeral {}

impl Ephemeral for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Ephemeral,
{
    /// Replace the ephemeral containers sub resource entirely.
    ///
    /// This functions in the same way as [`Api::replace`] except only `.spec.ephemeralcontainers` is replaced, everything else is ignored.
    ///
    /// Note that ephemeral containers may **not** be changed or removed once attached to a pod.
    ///
    ///
    /// You way want to patch the underlying resource to gain access to the main container process,
    /// see the [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/) for `sharedProcessNamespace`.
    ///
    /// See the Kubernetes [documentation](https://kubernetes.io/docs/concepts/workloads/pods/ephemeral-containers/#what-is-an-ephemeral-container) for more details.
    ///
    /// [`Api::patch_ephemeral_containers`] may be more ergonomic, as you can will avoid having to first fetch the
    /// existing subresources with an appropriate merge strategy, see the examples for more details.
    ///
    /// Example of using `replace_ephemeral_containers`:
    ///
    /// ```no_run
    /// use k8s_openapi::api::core::v1::Pod;
    /// use kube::{Api, api::PostParams};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let pp = PostParams::default();
    ///
    /// // Get pod object with ephemeral containers.
    /// let mut mypod = pods.get_ephemeral_containers("mypod").await?;
    ///
    /// // If there were existing ephemeral containers, we would have to append
    /// // new containers to the list before calling replace_ephemeral_containers.
    /// assert_eq!(mypod.spec.as_mut().unwrap().ephemeral_containers, None);
    ///
    /// // Add an ephemeral container to the pod object.
    /// mypod.spec.as_mut().unwrap().ephemeral_containers = Some(serde_json::from_value(serde_json::json!([
    ///    {
    ///        "name": "myephemeralcontainer",
    ///        "image": "busybox:stable",
    ///        "command": ["sh", "-c", "sleep 20"],
    ///    },
    /// ]))?);
    ///
    /// pods.replace_ephemeral_containers("mypod", &pp, &mypod).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replace_ephemeral_containers(&self, name: &str, pp: &PostParams, data: &K) -> Result<K>
    where
        K: Serialize,
    {
        let result = wit_api::replace_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            self.convert_post_params(pp),
            wit_api::Scope::Subresource("ephemeralcontainers".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Patch the ephemeral containers sub resource
    ///
    /// Any partial object containing the ephemeral containers
    /// sub resource is valid as long as the complete structure
    /// for the object is present, as shown below.
    ///
    /// You way want to patch the underlying resource to gain access to the main container process,
    /// see the [docs](https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/) for `sharedProcessNamespace`.
    ///
    /// Ephemeral containers may **not** be changed or removed once attached to a pod.
    /// Therefore if the chosen merge strategy overwrites the existing ephemeral containers,
    /// you will have to fetch the existing ephemeral containers first.
    /// In order to append your new ephemeral containers to the existing list before patching. See some examples and
    /// discussion related to merge strategies in Kubernetes
    /// [here](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment). The example below uses a strategic merge patch which does not require
    ///
    /// See the `Kubernetes` [documentation](https://kubernetes.io/docs/concepts/workloads/pods/ephemeral-containers/)
    /// for more information about ephemeral containers.
    ///
    ///
    /// Example of using `patch_ephemeral_containers`:
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch};
    /// use k8s_openapi::api::core::v1::Pod;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let pp = PatchParams::default(); // stratetgic merge patch
    ///
    /// // Note that the strategic merge patch will concatenate the
    /// // lists of ephemeral containers so we avoid having to fetch the
    /// // current list and append to it manually.
    /// let patch = serde_json::json!({
    ///    "spec":{
    ///    "ephemeralContainers": [
    ///    {
    ///        "name": "myephemeralcontainer",
    ///        "image": "busybox:stable",
    ///        "command": ["sh", "-c", "sleep 20"],
    ///    },
    ///    ]
    /// }});
    ///
    /// pods.patch_ephemeral_containers("mypod", &pp, &Patch::Strategic(patch)).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn patch_ephemeral_containers<P: serde::Serialize>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let patch_str = match patch {
            Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
        };

        let result = wit_api::patch_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            self.get_wit_patch_type(patch),
            patch_str,
            self.convert_patch_params(pp),
            wit_api::Scope::Subresource("ephemeralcontainers".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Get the named resource with the ephemeral containers subresource.
    ///
    /// This returns the whole K, with metadata and spec.
    pub async fn get_ephemeral_containers(&self, name: &str) -> Result<K> {
        let result = wit_api::get_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            None,
            wit_api::Scope::Subresource("ephemeralcontainers".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

// ----------------------------------------------------------------------------
// Resize subresource
// ----------------------------------------------------------------------------

/// Marker trait for objects that support the resize sub resource.
///
/// The resize subresource allows updating container resource requests/limits
/// without restarting the pod. This is available in Kubernetes 1.33+.
///
/// See [`Api::get_resize`], [`Api::patch_resize`], and [`Api::replace_resize`].
///
/// See the Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)
/// and [limitations](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#limitations)
/// for more details.
#[cfg_attr(docsrs, doc(cfg(feature = "k8s_if_ge_1_33")))]
pub trait Resize {}

k8s_openapi::k8s_if_ge_1_33! {
    impl Resize for k8s_openapi::api::core::v1::Pod {}
}

k8s_openapi::k8s_if_ge_1_33! {
    impl<K> Api<K>
    where
        K: Clone + DeserializeOwned + Resize,
    {
        /// Get the named resource with the resize subresource.
        ///
        /// This returns the whole Pod object with current resource allocations.
        ///
        /// See the Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)
        /// and [limitations](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#limitations)
        /// for more details.
        pub async fn get_resize(&self, name: &str) -> Result<K> {
            let result = wit_api::get_resource(
                self.get_wit_api_resource(),
                name.to_string(),
                None,
                wit_api::Scope::Subresource("resize".to_string()),
            )
            .await?;

            serde_json::from_str(&result).map_err(|e| {
                tracing::warn!("{}, {:?}", result, e);
                Error::SerdeError(e)
            })
        }

        /// Patch the resize sub resource.
        ///
        /// This allows you to update specific container resource requirements
        /// without fetching the entire Pod object first.
        ///
        /// Note that only certain container resource fields can be modified. See the
        /// [limitations](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#limitations)
        /// for details on what can be changed.
        ///
        /// # Example
        ///
        /// ```no_run
        /// use kube::api::{Api, PatchParams, Patch};
        /// use k8s_openapi::api::core::v1::Pod;
        /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
        /// # let client = kube::Client::try_default().await?;
        /// let pods: Api<Pod> = Api::namespaced(client, "default");
        /// let pp = PatchParams::default();
        ///
        /// let patch = serde_json::json!({
        ///     "spec": {
        ///         "containers": [{
        ///             "name": "mycontainer",
        ///             "resources": {
        ///                 "requests": {
        ///                     "cpu": "200m",
        ///                     "memory": "512Mi"
        ///                 }
        ///             }
        ///         }]
        ///     }
        /// });
        ///
        /// pods.patch_resize("mypod", &pp, &Patch::Strategic(patch)).await?;
        /// # Ok(())
        /// # }
        /// ```
        pub async fn patch_resize<P: serde::Serialize>(
            &self,
            name: &str,
            pp: &PatchParams,
            patch: &Patch<P>,
        ) -> Result<K> {
            let patch_str = match patch {
                Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
                #[cfg(feature = "jsonpatch")]
                Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
                Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
                Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
                _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
            };

            let result = wit_api::patch_resource(
                self.get_wit_api_resource(),
                name.to_string(),
                self.get_wit_patch_type(patch),
                patch_str,
                self.convert_patch_params(pp),
                wit_api::Scope::Subresource("resize".to_string()),
            )
            .await?;

            serde_json::from_str(&result).map_err(|e| {
                tracing::warn!("{}, {:?}", result, e);
                Error::SerdeError(e)
            })
        }

        /// Replace the resize sub resource entirely.
        ///
        /// This works similarly to [`Api::replace`] but uses the resize subresource.
        /// Takes a full Pod object with updated container resource requirements.
        ///
        /// Note that only certain container resource fields can be modified. See the
        /// [limitations](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#limitations)
        /// for details on what can be changed.
        ///
        /// # Example
        ///
        /// ```no_run
        /// use k8s_openapi::api::core::v1::Pod;
        /// use kube::{Api, api::PostParams};
        /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
        /// # let client = kube::Client::try_default().await?;
        /// let pods: Api<Pod> = Api::namespaced(client, "default");
        /// let pp = PostParams::default();
        ///
        /// // Get current pod
        /// let mut pod = pods.get("mypod").await?;
        ///
        /// // Modify resource requirements
        /// if let Some(spec) = &mut pod.spec &&
        ///    let Some(container) = spec.containers.get_mut(0) &&
        ///    let Some(resources) = &mut container.resources {
        ///         // Update CPU/memory limits or requests
        ///         // ...
        /// }
        ///
        /// pods.replace_resize("mypod", &pp, &pod).await?;
        /// # Ok(())
        /// # }
        /// ```
        pub async fn replace_resize(&self, name: &str, pp: &PostParams, data: &K) -> Result<K>
        where
            K: Serialize,
        {
            let result = wit_api::replace_resource(
                self.get_wit_api_resource(),
                name.to_string(),
                serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
                self.convert_post_params(pp),
                wit_api::Scope::Subresource("resize".to_string()),
            )
            .await?;

            serde_json::from_str(&result).map_err(|e| {
                tracing::warn!("{}, {:?}", result, e);
                Error::SerdeError(e)
            })
        }
    }
}

// ----------------------------------------------------------------------------

// TODO: Replace examples with owned custom resources. Bad practice to write to owned objects
// These examples work, but the job controller will totally overwrite what we do.
/// Methods for [status subresource](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#status-subresource).
impl<K> Api<K>
where
    K: DeserializeOwned,
{
    /// Get the named resource with a status subresource
    ///
    /// This actually returns the whole K, with metadata, and spec.
    pub async fn get_status(&self, name: &str) -> Result<K> {
        let result = wit_api::get_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            None,
            wit_api::Scope::Subresource("status".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Patch fields on the status object
    ///
    /// NB: Requires that the resource has a status subresource.
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch};
    /// use k8s_openapi::api::batch::v1::Job;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let mut j = jobs.get("baz").await?;
    /// let pp = PatchParams::default(); // json merge patch
    /// let data = serde_json::json!({
    ///     "status": {
    ///         "succeeded": 2
    ///     }
    /// });
    /// let o = jobs.patch_status("baz", &pp, &Patch::Merge(data)).await?;
    /// assert_eq!(o.status.unwrap().succeeded, Some(2));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn patch_status<P: serde::Serialize + Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let patch_str = match patch {
            Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
        };

        let result = wit_api::patch_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            self.get_wit_patch_type(patch),
            patch_str,
            self.convert_patch_params(pp),
            wit_api::Scope::Subresource("status".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Replace every field on the status object
    ///
    /// This works similarly to the [`Api::replace`] method, but `.spec` is ignored.
    /// You can leave out the `.spec` entirely from the serialized output.
    ///
    /// ```no_run
    /// use kube::api::{Api, PostParams};
    /// use k8s_openapi::api::batch::v1::{Job, JobStatus};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// #   let client = kube::Client::try_default().await?;
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let mut o = jobs.get_status("baz").await?; // retrieve partial object
    /// o.status = Some(JobStatus::default()); // update the job part
    /// let pp = PostParams::default();
    /// let o = jobs.replace_status("baz", &pp, &o).await?;
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn replace_status(&self, name: &str, pp: &PostParams, data: &K) -> Result<K>
    where
        K: Serialize,
    {
        let result = wit_api::replace_resource(
            self.get_wit_api_resource(),
            name.to_string(),
            serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            self.convert_post_params(pp),
            wit_api::Scope::Subresource("status".to_string()),
        )
        .await?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

// ----------------------------------------------------------------------------
// Log subresource
// ----------------------------------------------------------------------------

/// Marker trait for objects that has logs
///
/// See [`Api::logs`] and [`Api::log_stream`] for usage.
pub trait Log {}

impl Log for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: DeserializeOwned + Log,
{
    /// Fetch logs as a string
    pub async fn logs(&self, name: &str, lp: &LogParams) -> Result<String> {
        wit_api::get_logs_string(
            self.get_wit_api_resource(),
            name.to_string(),
            self.convert_log_params(lp),
        )
        .await
        .map_err(Error::Wit)
    }

    /// Stream the logs via [`AsyncBufRead`].
    ///
    /// Log stream can be processed using [`AsyncReadExt`](futures::AsyncReadExt)
    /// and [`AsyncBufReadExt`](futures::AsyncBufReadExt).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use kube::{api::{Api, LogParams}, Client};
    /// # let client: Client = todo!();
    /// use futures::{AsyncBufReadExt, TryStreamExt};
    ///
    /// let pods: Api<Pod> = Api::default_namespaced(client);
    /// let mut logs = pods
    ///     .log_stream("my-pod", &LogParams::default()).await?
    ///     .lines();
    ///
    /// while let Some(line) = logs.try_next().await? {
    ///     println!("{}", line);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn log_stream(&self, _name: &str, _lp: &LogParams) -> Result<impl AsyncBufRead + use<K>> {
        todo!("Implement log streaming via wasi. This is not implemented yet.");
        #[allow(unreachable_code)]
        Ok(futures::io::empty())
    }
}

// ----------------------------------------------------------------------------
// Eviction subresource
// ----------------------------------------------------------------------------

/// Marker trait for objects that can be evicted
///
/// See [`Api::evic`] for usage
pub trait Evict {}

impl Evict for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: DeserializeOwned + Evict,
{
    /// Create an eviction
    pub async fn evict(&self, name: &str, ep: &EvictParams) -> Result<Status> {
        let result_val = wit_api::evict_subresource(
            self.get_wit_api_resource(),
            name.to_string(),
            self.convert_evict_params(ep),
        )
        .await
        .map_err(Error::Wit)?;

        serde_json::from_str(&result_val).map_err(|e| {
            tracing::warn!("{}, {:?}", result_val, e);
            Error::SerdeError(e)
        })
    }
}
