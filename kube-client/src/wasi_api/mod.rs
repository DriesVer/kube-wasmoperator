//! API helpers for structured interaction with the Kubernetes API

mod core_methods;
#[cfg(feature = "ws")]
mod remote_command;
use std::fmt::Debug;

#[cfg(feature = "ws")]
pub use remote_command::{AttachedProcess, TerminalSize};
#[cfg(feature = "ws")]
mod portforward;
#[cfg(feature = "ws")]
pub use portforward::Portforwarder;

mod subresource;
#[cfg_attr(docsrs, doc(cfg(feature = "k8s_if_ge_1_33")))]
pub use subresource::Resize;
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub use subresource::{Attach, AttachParams, Ephemeral, Execute, Portforward};
pub use subresource::{Evict, EvictParams, Log, LogParams, ScaleSpec, ScaleStatus};

mod util;

pub mod entry;

// Re-exports from kube-core
#[cfg(feature = "admission")]
#[cfg_attr(docsrs, doc(cfg(feature = "admission")))]
pub use kube_core::admission;
pub(crate) use kube_core::params;
use kube_core::{DynamicResourceScope, NamespaceResourceScope};
pub use kube_core::{
    Resource, ResourceExt,
    dynamic::{ApiResource, DynamicObject},
    gvk::{GroupVersionKind, GroupVersionResource},
    metadata::{ListMeta, ObjectMeta, PartialObjectMeta, PartialObjectMetaExt, TypeMeta},
    object::{NotUsed, Object, ObjectList},
    watch::WatchEvent,
};
pub use params::{
    DeleteParams, GetParams, ListParams, Patch, PatchParams, PostParams, Preconditions, PropagationPolicy,
    ValidationDirective, VersionMatch, WatchParams,
};

use crate::Client;
/// The generic Api abstraction
///
/// This abstracts over a [`Request`] and a type `K` so that
/// we get automatic serialization/deserialization on the api calls
/// implemented by the dynamic [`Resource`].
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
#[derive(Clone)]
pub struct Api<K> {
    /// The client to use (from this library)
    pub(crate) client: Client,
    namespace: Option<String>,
    /// Whether requests should use metadata-only Accept headers
    /// (cached from `K::metadata_api()` at construction so that `impl<K> Api<K>`
    /// method blocks don't have to tighten to `K: Resource`).
    pub(crate) metadata_api: bool,
    /// Note: Using `iter::Empty` over `PhantomData`, because we never actually keep any
    /// `K` objects, so `Empty` better models our constraints (in particular, `Empty<K>`
    /// is `Send`, even if `K` may not be).
    pub(crate) _phantom: std::iter::Empty<K>,
    pub(crate) group: String,
    pub(crate) version: String,
    pub(crate) kind: String,
    pub(crate) plural: String,
}

/// Api constructors for Resource implementors with custom DynamicTypes
///
/// This generally means resources created via [`DynamicObject`](crate::api::DynamicObject).
impl<K: Resource> Api<K> {
    /// Return a reference to the namespace of this [`Api`] instance, if any
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Cluster level resources, or resources viewed across all namespaces
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    ///
    /// # Warning
    ///
    /// This variant **can only `list` and `watch` namespaced resources** and is commonly used with a `watcher`.
    /// If you need to create/patch/replace/get on a namespaced resource, you need a separate `Api::namespaced`.
    pub fn all_with(client: Client, dyntype: &K::DynamicType) -> Self {
        Self {
            client,
            namespace: None,
            metadata_api: K::metadata_api(),
            _phantom: std::iter::empty(),
            group: K::group(dyntype).to_string(),
            version: K::version(dyntype).to_string(),
            kind: K::kind(dyntype).to_string(),
            plural: K::plural(dyntype).to_string(),
        }
    }

    /// Namespaced resource within a given namespace
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    pub fn namespaced_with(client: Client, ns: &str, dyntype: &K::DynamicType) -> Self
    where
        K: Resource<Scope = DynamicResourceScope>,
    {
        Self {
            client,
            namespace: Some(ns.to_string()),
            metadata_api: K::metadata_api(),
            _phantom: std::iter::empty(),
            group: K::group(dyntype).to_string(),
            version: K::version(dyntype).to_string(),
            kind: K::kind(dyntype).to_string(),
            plural: K::plural(dyntype).to_string(),
        }
    }

    /// Namespaced resource within the default namespace
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    ///
    /// The namespace is either configured on `context` in the kubeconfig
    /// or falls back to `default` when running locally, and it's using the service account's
    /// namespace when deployed in-cluster.
    pub fn default_namespaced_with(client: Client, dyntype: &K::DynamicType) -> Self
    where
        K: Resource<Scope = DynamicResourceScope>,
    {
        let ns = client.default_namespace().to_string();
        Self::namespaced_with(client, &ns, dyntype)
    }

    /// Consume self and return the [`Client`]
    pub fn into_client(self) -> Client {
        self.into()
    }
}

/// Api constructors for Resource implementors with Default DynamicTypes
///
/// This generally means structs implementing `k8s_openapi::Resource`.
impl<K: Resource> Api<K>
where
    <K as Resource>::DynamicType: Default,
{
    /// Cluster level resources, or resources viewed across all namespaces
    ///
    /// Namespace scoped resource allowing querying across all namespaces:
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::all(client);
    /// ```
    ///
    /// Cluster scoped resources also use this entrypoint:
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::all(client);
    /// ```
    ///
    /// # Warning
    ///
    /// This variant **can only `list` and `watch` namespaced resources** and is commonly used with a `watcher`.
    /// If you need to create/patch/replace/get on a namespaced resource, you need a separate `Api::namespaced`.
    pub fn all(client: Client) -> Self {
        Self::all_with(client, &K::DynamicType::default())
    }

    /// Namespaced resource within a given namespace
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::namespaced(client, "default");
    /// ```
    ///
    /// This will ONLY work on namespaced resources as set by `Scope`:
    ///
    /// ```compile_fail
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::namespaced(client, "default"); // resource not namespaced!
    /// ```
    ///
    /// For dynamic type information, use [`Api::namespaced_with`] variants.
    pub fn namespaced(client: Client, ns: &str) -> Self
    where
        K: Resource<Scope = NamespaceResourceScope>,
    {
        let dyntype = K::DynamicType::default();
        Self {
            client,
            namespace: Some(ns.to_string()),
            metadata_api: K::metadata_api(),
            _phantom: std::iter::empty(),
            group: K::group(&dyntype).to_string(),
            version: K::version(&dyntype).to_string(),
            kind: K::kind(&dyntype).to_string(),
            plural: K::plural(&dyntype).to_string(),
        }
    }

    /// Namespaced resource within the default namespace
    ///
    /// The namespace is either configured on `context` in the kubeconfig
    /// or falls back to `default` when running locally, and it's using the service account's
    /// namespace when deployed in-cluster.
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::default_namespaced(client);
    /// ```
    ///
    /// This will ONLY work on namespaced resources as set by `Scope`:
    ///
    /// ```compile_fail
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::default_namespaced(client); // resource not namespaced!
    /// ```
    pub fn default_namespaced(client: Client) -> Self
    where
        K: Resource<Scope = NamespaceResourceScope>,
    {
        let ns = client.default_namespace().to_string();
        Self::namespaced(client, &ns)
    }
}

impl<K> From<Api<K>> for Client {
    fn from(api: Api<K>) -> Self {
        api.client
    }
}

impl<K> Debug for Api<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Intentionally destructuring, to cause compile errors when new fields are added
        let Self {
            client: _,
            namespace,
            metadata_api: _,
            _phantom,
            group: _,
            version: _,
            kind: _,
            plural: _,
        } = self;
        f.debug_struct("Api")
            .field("client", &"...")
            .field("namespace", &namespace)
            .finish()
    }
}
