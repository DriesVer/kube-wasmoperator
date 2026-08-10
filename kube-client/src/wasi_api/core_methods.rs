use either::Either;
use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use crate::{Api, Error, EvictParams, LogParams, Result, wit_api};
use kube_core::{WatchEvent, metadata::PartialObjectMeta, object::ObjectList, params::*, response::Status};

/// PUSH/PUT/POST/GET abstractions
impl<K> Api<K> {
    pub(crate) fn get_wit_api_resource(&self) -> wit_api::ApiResource {
        wit_api::ApiResource {
            group: self.group.clone(),
            version: self.version.clone(),
            kind: self.kind.clone(),
            plural: self.plural.clone(),
            namespace: self.namespace.clone(),
        }
    }

    pub(crate) fn get_wit_api_scope(&self) -> wit_api::Scope {
        if self.metadata_api {
            wit_api::Scope::MetadataOnly
        } else {
            wit_api::Scope::Full
        }
    }

    pub(crate) fn get_wit_patch_type<P>(&self, kube_patch: &Patch<P>) -> wit_api::PatchType
    where
        P: serde::Serialize,
    {
        match kube_patch {
            Patch::Apply(_) => wit_api::PatchType::Apply,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(_) => wit_api::PatchType::Json,
            Patch::Merge(_) => wit_api::PatchType::Merge,
            Patch::Strategic(_) => wit_api::PatchType::Strategic,
            _ => wit_api::PatchType::Merge,
        }
    }

    pub(crate) fn convert_post_params(&self, pp: &PostParams) -> wit_api::CreateParams {
        wit_api::CreateParams {
            dry_run: pp.dry_run,
            field_manager: pp.field_manager.clone(),
        }
    }

    pub(crate) fn convert_propagation_policy(&self, p: &PropagationPolicy) -> wit_api::PropagationPolicy {
        match p {
            PropagationPolicy::Orphan => wit_api::PropagationPolicy::Orphan,
            PropagationPolicy::Background => wit_api::PropagationPolicy::Background,
            PropagationPolicy::Foreground => wit_api::PropagationPolicy::Foreground,
        }
    }

    pub(crate) fn convert_preconditions(&self, p: &Preconditions) -> wit_api::Preconditions {
        wit_api::Preconditions {
            uid: p.uid.clone(),
            resource_version: p.resource_version.clone(),
        }
    }

    pub(crate) fn convert_delete_params(&self, dp: &DeleteParams) -> wit_api::DeleteParams {
        wit_api::DeleteParams {
            dry_run: dp.dry_run,
            grace_period_seconds: dp.grace_period_seconds,
            propagation_policy: dp
                .propagation_policy
                .as_ref()
                .map(|p| self.convert_propagation_policy(p)),
            preconditions: dp.preconditions.as_ref().map(|p| self.convert_preconditions(p)),
        }
    }

    pub(crate) fn convert_list_params(&self, lp: &ListParams) -> wit_api::ListParams {
        wit_api::ListParams {
            label_selector: lp.label_selector.clone(),
            field_selector: lp.field_selector.clone(),
            timeout: lp.timeout,
            limit: lp.limit,
            continue_token: lp.continue_token.clone(),
            version_match: lp.version_match.as_ref().map(|vm| match vm {
                VersionMatch::Exact => wit_api::VersionMatch::Exact,
                VersionMatch::NotOlderThan => wit_api::VersionMatch::NotLater,
            }),
            resource_version: lp.resource_version.clone(),
        }
    }

    pub(crate) fn convert_validation_directive(
        &self,
        vd: &ValidationDirective,
    ) -> wit_api::ValidationDirective {
        match vd {
            ValidationDirective::Strict => wit_api::ValidationDirective::Strict,
            ValidationDirective::Warn => wit_api::ValidationDirective::Warn,
            ValidationDirective::Ignore => wit_api::ValidationDirective::Ignore,
        }
    }

    pub(crate) fn convert_patch_params(&self, pp: &PatchParams) -> wit_api::PatchParams {
        wit_api::PatchParams {
            dry_run: pp.dry_run,
            field_manager: pp.field_manager.clone(),
            field_validation: pp
                .field_validation
                .as_ref()
                .map(|vd| self.convert_validation_directive(vd)),
            force: pp.force,
        }
    }

    pub(crate) fn convert_watch_params(&self, wp: &WatchParams) -> wit_api::WatchParams {
        wit_api::WatchParams {
            label_selector: wp.label_selector.clone(),
            field_selector: wp.field_selector.clone(),
            bookmark: wp.bookmarks,
            send_initial_events: wp.send_initial_events,
        }
    }

    pub(crate) fn convert_log_params(&self, lp: &LogParams) -> wit_api::LogParams {
        wit_api::LogParams {
            container: lp.container.clone(),
            follow: lp.follow,
            limit_bytes: lp.limit_bytes.map(|x| x as u64),
            pretty: lp.pretty,
            previous: lp.previous,
            since_seconds: lp.since_seconds.map(|x| x as u64),
            since_time: lp.since_time.as_ref().map(|t: &jiff::Timestamp| t.to_string()),
            tail_lines: lp.tail_lines.map(|x| x as u64),
            timestamps: lp.timestamps,
        }
    }

    pub(crate) fn convert_evict_params(&self, ep: &EvictParams) -> wit_api::EvictParams {
        wit_api::EvictParams {
            delete_options: ep
                .delete_options
                .as_ref()
                .map(|dp| self.convert_delete_params(dp)),
            post_options: wit_api::CreateParams {
                dry_run: ep.post_options.dry_run,
                field_manager: None,
            },
        }
    }
}

/// PUSH/PUT/POST/GET abstractions
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Debug,
{
    /// Get a named resource
    ///
    /// ```no_run
    /// # use kube::Api;
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let p: Pod = pods.get("blog").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// This function assumes that the object is expected to always exist, and returns [`Error`] if it does not.
    /// Consider using [`Api::get_opt`] if you need to handle missing objects.
    pub async fn get(&self, name: &str) -> Result<K> {
        self.get_with(name, &GetParams::default()).await
    }

    ///  Get only the metadata for a named resource as [`PartialObjectMeta`]
    ///
    /// ```no_run
    /// use kube::{Api, core::PartialObjectMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let p: PartialObjectMeta<Pod> = pods.get_metadata("blog").await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Note that the type may be converted to `ObjectMeta` through the usual
    /// conversion traits.
    ///
    /// # Errors
    ///
    /// This function assumes that the object is expected to always exist, and returns [`Error`] if it does not.
    /// Consider using [`Api::get_metadata_opt`] if you need to handle missing objects.
    pub async fn get_metadata(&self, name: &str) -> Result<PartialObjectMeta<K>> {
        self.get_metadata_with(name, &GetParams::default()).await
    }

    /// [Get](`Api::get`) a named resource with an explicit resourceVersion
    ///
    /// This function allows the caller to pass in a [`GetParams`](`super::GetParams`) type containing
    /// a `resourceVersion` to a [Get](`Api::get`) call.
    /// For example
    ///
    /// ```no_run
    /// # use kube::{Api, api::GetParams};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let p: Pod = pods.get_with("blog", &GetParams::any()).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// This function assumes that the object is expected to always exist, and returns [`Error`] if it does not.
    /// Consider using [`Api::get_opt`] if you need to handle missing objects.
    pub async fn get_with(&self, name: &str, gp: &GetParams) -> Result<K> {
        let result = wit_api::get_resource(
            &self.get_wit_api_resource(),
            name,
            gp.resource_version.as_deref(),
            &self.get_wit_api_scope(),
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    ///  [Get](`Api::get_metadata`) the metadata of an object using an explicit `resourceVersion`
    ///
    /// This function allows the caller to pass in a [`GetParams`](`super::GetParams`) type containing
    /// a `resourceVersion` to a [Get](`Api::get_metadata`) call.
    /// For example
    ///
    ///
    /// ```no_run
    /// use kube::{Api, api::GetParams, core::PartialObjectMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let p: PartialObjectMeta<Pod> = pods.get_metadata_with("blog", &GetParams::any()).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Note that the type may be converted to `ObjectMeta` through the usual
    /// conversion traits.
    ///
    /// # Errors
    ///
    /// This function assumes that the object is expected to always exist, and returns [`Error`] if it does not.
    /// Consider using [`Api::get_metadata_opt`] if you need to handle missing objects.
    pub async fn get_metadata_with(&self, name: &str, gp: &GetParams) -> Result<PartialObjectMeta<K>> {
        let result = wit_api::get_resource(
            &self.get_wit_api_resource(),
            name,
            gp.resource_version.as_deref(),
            &wit_api::Scope::MetadataOnly,
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// [Get](`Api::get`) a named resource if it exists, returns [`None`] if it doesn't exist
    ///
    /// ```no_run
    /// # use kube::Api;
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// if let Some(pod) = pods.get_opt("blog").await? {
    ///     // Pod was found
    /// } else {
    ///     // Pod was not found
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_opt(&self, name: &str) -> Result<Option<K>> {
        match self.get(name).await {
            Ok(obj) => Ok(Some(obj)),
            Err(Error::Wit(wit_api::Error::NotFound)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// [Get Metadata](`Api::get_metadata`) for a named resource if it exists, returns [`None`] if it doesn't exist
    ///
    /// ```no_run
    /// # use kube::Api;
    /// use k8s_openapi::api::core::v1::Pod;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// if let Some(pod) = pods.get_metadata_opt("blog").await? {
    ///     // Pod was found
    /// } else {
    ///     // Pod was not found
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that [`PartialObjectMeta`] embeds the raw `ObjectMeta`.
    pub async fn get_metadata_opt(&self, name: &str) -> Result<Option<PartialObjectMeta<K>>> {
        self.get_metadata_opt_with(name, &GetParams::default()).await
    }

    /// [Get Metadata](`Api::get_metadata`) of an object if it exists, using an explicit `resourceVersion`.
    /// Returns [`None`] if it doesn't exist.
    ///
    /// ```no_run
    /// # use kube::Api;
    /// use k8s_openapi::api::core::v1::Pod;
    /// use kube_core::params::GetParams;
    ///
    /// async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// if let Some(pod) = pods.get_metadata_opt_with("blog", &GetParams::any()).await? {
    ///     // Pod was found
    /// } else {
    ///     // Pod was not found
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that [`PartialObjectMeta`] embeds the raw `ObjectMeta`.
    pub async fn get_metadata_opt_with(
        &self,
        name: &str,
        gp: &GetParams,
    ) -> Result<Option<PartialObjectMeta<K>>> {
        match self.get_metadata_with(name, gp).await {
            Ok(meta) => Ok(Some(meta)),
            Err(Error::Wit(wit_api::Error::NotFound)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Get a list of resources
    ///
    /// You use this to get everything, or a subset matching fields/labels, say:
    ///
    /// ```no_run
    /// use kube::api::{Api, ListParams, ResourceExt};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let lp = ListParams::default().labels("app=blog"); // for this app only
    /// for p in pods.list(&lp).await? {
    ///     println!("Found Pod: {}", p.name_any());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(&self, lp: &ListParams) -> Result<ObjectList<K>> {
        let result = wit_api::list_resources(
            &self.get_wit_api_resource(),
            &self.convert_list_params(lp),
            &self.get_wit_api_scope(),
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Get a list of resources that contains only their metadata as
    ///
    /// Similar to [list](`Api::list`), you use this to get everything, or a
    /// subset matching fields/labels. For example
    ///
    /// ```no_run
    /// use kube::api::{Api, ListParams, ResourceExt};
    /// use kube::core::{ObjectMeta, ObjectList, PartialObjectMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let lp = ListParams::default().labels("app=blog"); // for this app only
    /// let list: ObjectList<PartialObjectMeta<Pod>> = pods.list_metadata(&lp).await?;
    /// for p in list {
    ///     println!("Found Pod: {}", p.name_any());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_metadata(&self, lp: &ListParams) -> Result<ObjectList<PartialObjectMeta<K>>> {
        let result = wit_api::list_resources(
            &self.get_wit_api_resource(),
            &self.convert_list_params(lp),
            &wit_api::Scope::MetadataOnly,
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Create a resource
    ///
    /// This function requires a type that Serializes to `K`, which can be:
    /// 1. Raw string YAML
    /// - easy to port from existing files
    ///     - error prone (run-time errors on typos due to failed serialize attempts)
    ///     - very error prone (can write invalid YAML)
    /// 2. An instance of the struct itself
    ///     - easy to instantiate for CRDs (you define the struct)
    ///     - dense to instantiate for [`k8s_openapi`] types (due to many optionals)
    ///     - compile-time safety
    ///     - but still possible to write invalid native types (validation at apiserver)
    /// 3. [`serde_json::json!`] macro instantiated [`serde_json::Value`]
    ///     - Tradeoff between the two
    ///     - Easy partially filling of native [`k8s_openapi`] types (most fields optional)
    ///     - Partial safety against runtime errors (at least you must write valid JSON)
    ///
    /// Note that this method cannot write to the status object (when it exists) of a resource.
    /// To set status objects please see [`Api::replace_status`] or [`Api::patch_status`].
    pub async fn create(&self, pp: &PostParams, data: &K) -> Result<K>
    where
        K: Serialize,
    {
        let result = wit_api::create_resource(
            &self.get_wit_api_resource(),
            &serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            &self.convert_post_params(pp),
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Delete a named resource
    ///
    /// When you get a `K` via `Left`, your delete has started.
    /// When you get a `Status` via `Right`, this should be a a 2XX style
    /// confirmation that the object being gone.
    ///
    /// 4XX and 5XX status types are returned as an [`Err(kube_client::Error::Api)`](crate::Error::Api).
    ///
    /// ```no_run
    /// use kube::api::{Api, DeleteParams};
    /// use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1 as apiexts;
    /// use apiexts::CustomResourceDefinition;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let crds: Api<CustomResourceDefinition> = Api::all(client);
    /// crds.delete("foos.clux.dev", &DeleteParams::default()).await?
    ///     .map_left(|o| println!("Deleting CRD: {:?}", o.status))
    ///     .map_right(|s| println!("Deleted CRD: {:?}", s));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, name: &str, dp: &DeleteParams) -> Result<Either<K, Status>> {
        let result = wit_api::delete_resource(
            &self.get_wit_api_resource(),
            name,
            &self.convert_delete_params(dp),
            &self.get_wit_api_scope(),
        )?;

        let val: serde_json::Value = serde_json::from_str(&result).map_err(Error::SerdeError)?;
        if val.get("kind").and_then(|v| v.as_str()) == Some("Status") {
            let status: Status = serde_json::from_value(val).map_err(Error::SerdeError)?;
            Ok(Either::Right(status))
        } else {
            let obj: K = serde_json::from_value(val).map_err(Error::SerdeError)?;
            Ok(Either::Left(obj))
        }
    }

    /// Delete a collection of resources
    ///
    /// When you get an `ObjectList<K>` via `Left`, your delete has started.
    /// When you get a `Status` via `Right`, this should be a a 2XX style
    /// confirmation that the object being gone.
    ///
    /// 4XX and 5XX status types are returned as an [`Err(kube_client::Error::Api)`](crate::Error::Api).
    ///
    /// ```no_run
    /// use kube::api::{Api, DeleteParams, ListParams, ResourceExt};
    /// use k8s_openapi::api::core::v1::Pod;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    ///
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// match pods.delete_collection(&DeleteParams::default(), &ListParams::default()).await? {
    ///     either::Left(list) => {
    ///         let names: Vec<_> = list.iter().map(ResourceExt::name_any).collect();
    ///         println!("Deleting collection of pods: {:?}", names);
    ///     },
    ///     either::Right(status) => {
    ///         println!("Deleted collection of pods: status={:?}", status);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_collection(
        &self,
        dp: &DeleteParams,
        lp: &ListParams,
    ) -> Result<Either<ObjectList<K>, Status>> {
        let result = wit_api::delete_collection(
            &self.get_wit_api_resource(),
            &self.convert_delete_params(dp),
            &self.convert_list_params(lp),
            &self.get_wit_api_scope(),
        )?;

        let val: serde_json::Value = serde_json::from_str(&result).map_err(Error::SerdeError)?;
        if val.get("kind").and_then(|v| v.as_str()) == Some("Status") {
            let status: Status = serde_json::from_value(val).map_err(Error::SerdeError)?;
            Ok(Either::Right(status))
        } else {
            let list: ObjectList<K> = serde_json::from_value(val).map_err(Error::SerdeError)?;
            Ok(Either::Left(list))
        }
    }

    /// Patch a subset of a resource's properties
    ///
    /// Takes a [`Patch`] along with [`PatchParams`] for the call.
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch, Resource};
    /// use k8s_openapi::api::core::v1::Pod;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    ///
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let patch = serde_json::json!({
    ///     "apiVersion": "v1",
    ///     "kind": "Pod",
    ///     "metadata": {
    ///         "name": "blog"
    ///     },
    ///     "spec": {
    ///         "activeDeadlineSeconds": 5
    ///     }
    /// });
    /// let params = PatchParams::apply("myapp");
    /// let patch = Patch::Apply(&patch);
    /// let o_patched = pods.patch("blog", &params, &patch).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// [`Patch`]: super::Patch
    /// [`PatchParams`]: super::PatchParams
    ///
    /// Note that this method cannot write to the status object (when it exists) of a resource.
    /// To set status objects please see [`Api::replace_status`] or [`Api::patch_status`].
    pub async fn patch<P: Serialize + Debug>(
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
            &self.get_wit_api_resource(),
            name,
            self.get_wit_patch_type(patch),
            &patch_str,
            &self.convert_patch_params(pp),
            &self.get_wit_api_scope(),
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Patch a metadata subset of a resource's properties from [`PartialObjectMeta`]
    ///
    /// Takes a [`Patch`] along with [`PatchParams`] for the call.
    /// Patches can be constructed raw using `serde_json::json!` or from `ObjectMeta` via [`PartialObjectMetaExt`].
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch, Resource};
    /// use kube::core::{PartialObjectMetaExt, ObjectMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let metadata = ObjectMeta {
    ///     labels: Some([("key".to_string(), "value".to_string())].into()),
    ///     ..Default::default()
    /// }.into_request_partial::<Pod>();
    ///
    /// let params = PatchParams::apply("myapp");
    /// let o_patched = pods.patch_metadata("blog", &params, &Patch::Apply(&metadata)).await?;
    /// println!("Patched {}", o_patched.metadata.name.unwrap());
    /// # Ok(())
    /// # }
    /// ```
    /// [`Patch`]: super::Patch
    /// [`PatchParams`]: super::PatchParams
    /// [`PartialObjectMetaExt`]: crate::core::PartialObjectMetaExt
    ///
    /// ### Warnings
    ///
    /// The `TypeMeta` (apiVersion + kind) of a patch request (required for apply patches)
    /// must match the underlying type that is being patched (e.g. "v1" + "Pod").
    /// The returned `TypeMeta` will always be {"meta.k8s.io/v1", "PartialObjectMetadata"}.
    /// These constraints are encoded into [`PartialObjectMetaExt`].
    ///
    /// This method can write to non-metadata fields such as spec if included in the patch.
    pub async fn patch_metadata<P: Serialize + Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<PartialObjectMeta<K>> {
        let patch_str = match patch {
            Patch::Apply(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            #[cfg(feature = "jsonpatch")]
            Patch::Json(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Strategic(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            Patch::Merge(p) => serde_json::to_string(p).map_err(Error::SerdeError)?,
            _ => return Err(Error::Wasi("Unsupported patch type".to_string())),
        };

        let result = wit_api::patch_resource(
            &self.get_wit_api_resource(),
            name,
            self.get_wit_patch_type(patch),
            &patch_str,
            &self.convert_patch_params(pp),
            &wit_api::Scope::MetadataOnly,
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Replace a resource entirely with a new one
    ///
    /// This is used just like [`Api::create`], but with one additional instruction:
    /// You must set `metadata.resourceVersion` in the provided data because k8s
    /// will not accept an update unless you actually knew what the last version was.
    ///
    /// Thus, to use this function, you need to do a `get` then a `replace` with its result.
    ///
    /// ```no_run
    /// use kube::api::{Api, PostParams, ResourceExt};
    /// use k8s_openapi::api::batch::v1::Job;
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let j = jobs.get("baz").await?;
    /// let j_new: Job = serde_json::from_value(serde_json::json!({
    ///     "apiVersion": "batch/v1",
    ///     "kind": "Job",
    ///     "metadata": {
    ///         "name": "baz",
    ///         "resourceVersion": j.resource_version(),
    ///     },
    ///     "spec": {
    ///         "template": {
    ///             "metadata": {
    ///                 "name": "empty-job-pod"
    ///             },
    ///             "spec": {
    ///                 "containers": [{
    ///                     "name": "empty",
    ///                     "image": "alpine:latest"
    ///                 }],
    ///                 "restartPolicy": "Never",
    ///             }
    ///         }
    ///     }
    /// }))?;
    /// jobs.replace("baz", &PostParams::default(), &j_new).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Consider mutating the result of `api.get` rather than recreating it.
    ///
    /// Note that this method cannot write to the status object (when it exists) of a resource.
    /// To set status objects please see [`Api::replace_status`] or [`Api::patch_status`].
    pub async fn replace(&self, name: &str, pp: &PostParams, data: &K) -> Result<K>
    where
        K: Serialize,
    {
        let result = wit_api::replace_resource(
            &self.get_wit_api_resource(),
            name,
            &serde_json::to_string(data).map_err(|e| Error::SerdeError(e))?,
            &self.convert_post_params(pp),
            &self.get_wit_api_scope(),
        )?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    /// Watch a list of resources
    ///
    /// This returns a future that awaits the initial response,
    /// then you can stream the remaining buffered `WatchEvent` objects.
    ///
    /// Note that a `watch` call can terminate for many reasons (even before the specified
    /// [`WatchParams::timeout`] is triggered), and will have to be re-issued
    /// with the last seen resource version when or if it closes.
    ///
    /// Consider using a managed [`watcher`] to deal with automatic re-watches and error cases.
    ///
    /// ```no_run
    /// use kube::api::{Api, WatchParams, ResourceExt, WatchEvent};
    /// use k8s_openapi::api::batch::v1::Job;
    /// use futures::{StreamExt, TryStreamExt};
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let lp = WatchParams::default()
    ///     .fields("metadata.name=my_job")
    ///     .timeout(20); // upper bound of how long we watch for
    /// let mut stream = jobs.watch(&lp, "0").await?.boxed();
    /// while let Some(status) = stream.try_next().await? {
    ///     match status {
    ///         WatchEvent::Added(s) => println!("Added {}", s.name_any()),
    ///         WatchEvent::Modified(s) => println!("Modified: {}", s.name_any()),
    ///         WatchEvent::Deleted(s) => println!("Deleted {}", s.name_any()),
    ///         WatchEvent::Bookmark(s) => {},
    ///         WatchEvent::Error(s) => println!("{}", s),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    /// [`WatchParams::timeout`]: super::WatchParams::timeout
    /// [`watcher`]: https://docs.rs/kube_runtime/*/kube_runtime/watcher/fn.watcher.html
    pub async fn watch(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> Result<impl Stream<Item = Result<WatchEvent<K>>> + use<K>>
    where
        K: DeserializeOwned + Debug + Send + 'static,
    {
        WatchStreamHandler::watch_resource(
            self.get_wit_api_resource(),
            self.convert_watch_params(wp),
            version,
            self.get_wit_api_scope(),
        )
    }

    /// Watch a list of metadata for a given resources
    ///
    /// This returns a future that awaits the initial response,
    /// then you can stream the remaining buffered `WatchEvent` objects.
    ///
    /// Note that a `watch_metadata` call can terminate for many reasons (even
    /// before the specified [`WatchParams::timeout`] is triggered), and will
    /// have to be re-issued with the last seen resource version when or if it
    /// closes.
    ///
    /// Consider using a managed [`metadata_watcher`] to deal with automatic re-watches and error cases.
    ///
    /// ```no_run
    /// use kube::api::{Api, WatchParams, ResourceExt, WatchEvent};
    /// use k8s_openapi::api::batch::v1::Job;
    /// use futures::{StreamExt, TryStreamExt};
    ///
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    ///
    /// let lp = WatchParams::default()
    ///     .fields("metadata.name=my_job")
    ///     .timeout(20); // upper bound of how long we watch for
    /// let mut stream = jobs.watch(&lp, "0").await?.boxed();
    /// while let Some(status) = stream.try_next().await? {
    ///     match status {
    ///         WatchEvent::Added(s) => println!("Added {}", s.metadata.name.unwrap()),
    ///         WatchEvent::Modified(s) => println!("Modified: {}", s.metadata.name.unwrap()),
    ///         WatchEvent::Deleted(s) => println!("Deleted {}", s.metadata.name.unwrap()),
    ///         WatchEvent::Bookmark(s) => {},
    ///         WatchEvent::Error(s) => println!("{}", s),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    /// [`WatchParams::timeout`]: super::WatchParams::timeout
    /// [`metadata_watcher`]: https://docs.rs/kube_runtime/*/kube_runtime/watcher/fn.metadata_watcher.html
    pub async fn watch_metadata(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> Result<impl Stream<Item = Result<WatchEvent<PartialObjectMeta<K>>>> + use<K>>
    where
        K: DeserializeOwned + Debug + Send + 'static,
    {
        WatchStreamHandler::watch_resource(
            self.get_wit_api_resource(),
            self.convert_watch_params(wp),
            version,
            wit_api::Scope::MetadataOnly,
        )
    }
}

// Watch stream handler
// Maybe move inside Api<K>

use dashmap::DashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

type WatchId = wit_api::WatchId;

trait WatchEventDispatcher: Send + Sync {
    fn dispatch(&self, event: wit_api::WatchEvent) -> Result<(), Error>;
}

impl<K> WatchEventDispatcher for UnboundedSender<Result<WatchEvent<K>>>
where
    K: DeserializeOwned + Send + 'static,
{
    fn dispatch(&self, event: wit_api::WatchEvent) -> Result<(), Error> {
        let parse = |s: &str| -> Result<K, Error> {
            serde_json::from_str::<K>(s).map_err(|e| {
                tracing::warn!("Failed to deserialize watch event payload ({}): {:?}", s, e);
                Error::SerdeError(e)
            })
        };

        let kube_event: Result<WatchEvent<K>> = match event {
            wit_api::WatchEvent::Added(s) => parse(&s).map(WatchEvent::Added),
            wit_api::WatchEvent::Modified(s) => parse(&s).map(WatchEvent::Modified),
            wit_api::WatchEvent::Deleted(s) => parse(&s).map(WatchEvent::Deleted),
            wit_api::WatchEvent::Bookmark(s) => serde_json::from_str::<kube_core::watch::Bookmark>(&s)
                .map(WatchEvent::Bookmark)
                .map_err(|e| {
                    tracing::warn!("Failed to deserialize bookmark JSON: {}, error: {:?}", s, e);
                    Error::SerdeError(e)
                }),
            wit_api::WatchEvent::Error(wit_error) => {
                let status: Status = serde_json::from_value(serde_json::json!({
                    "status": "Failure",
                    "message": format!("wit_error: {:?}", wit_error),
                    "reason": "WitError",
                    "code": 500,
                }))
                .map_err(|e| Error::SerdeError(e))?;
                Ok(WatchEvent::Error(Box::new(status)))
            }
        };

        // TODO: Better error type needed
        self.send(kube_event).map_err(|e| Error::Wasi(e.to_string()))?;

        Ok(())
    }
}

struct WatchStreamHandler {
    pub map: DashMap<WatchId, Box<dyn WatchEventDispatcher>>,
}

static WATCH_STREAM_HANDLER: LazyLock<Arc<WatchStreamHandler>> =
    LazyLock::new(|| Arc::new(WatchStreamHandler { map: DashMap::new() }));

impl WatchStreamHandler {
    pub fn get_instance() -> Arc<Self> {
        Arc::clone(&WATCH_STREAM_HANDLER)
    }

    pub fn watch_resource<K>(
        api: wit_api::ApiResource,
        watch_params: wit_api::WatchParams,
        version: &str,
        scope: wit_api::Scope,
    ) -> Result<impl Stream<Item = Result<WatchEvent<K>>> + use<K>>
    where
        K: Clone + DeserializeOwned + Debug + Send + 'static,
    {
        // Start a new watch stream for the resource on the host
        let id = wit_api::subscribe_watch_stream(&api, version, &watch_params, &scope)?;

        // Create a new channel and stream for the watch events
        let (tx, rx): (
            UnboundedSender<Result<WatchEvent<K>>>,
            UnboundedReceiver<Result<WatchEvent<K>>>,
        ) = mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(rx);

        // Store the sender in the map for later use
        WATCH_STREAM_HANDLER.map.insert(id.clone(), Box::new(tx));

        Ok(stream)
    }
}

pub struct WatchStreamReceiver;

impl crate::Guest for WatchStreamReceiver {
    fn receive_watch_event(
        watch_id: wit_api::WatchId,
        event: wit_api::WatchEvent,
    ) -> Result<(), wit_api::Error> {
        let stream_handler = WatchStreamHandler::get_instance();

        let dispatcher = stream_handler
            .map
            .get(&watch_id)
            .ok_or_else(|| wit_api::Error::NotFound)?;

        dispatcher
            .dispatch(event)
            .map_err(|e| wit_api::Error::Other(e.to_string()))
    }
}
