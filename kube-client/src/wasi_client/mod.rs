use jiff::Timestamp;
use k8s_openapi::apimachinery::pkg::apis::meta::v1 as k8s_meta_v1;
use serde_json;

pub use self::body::Body;
use crate::{Config, Error, Result, wit_api};

pub use builder::ClientBuilder;

pub use kube_core::discovery::v2::{
    APIGroupDiscovery, APIGroupDiscoveryList, APIResourceDiscovery, APISubresourceDiscovery,
    APIVersionDiscovery, GroupVersionKind as DiscoveryGroupVersionKind,
};

mod body;
mod builder;
mod config_ext;

pub use config_ext::ConfigExt;

pub type AuthError = std::convert::Infallible;

#[derive(Clone)]
pub struct Client {
    default_ns: String,
    valid_until: Option<Timestamp>,
}

impl Client {
    pub(crate) fn new_raw(default_ns: String, valid_until: Option<Timestamp>) -> Self {
        Self {
            default_ns,
            valid_until,
        }
    }

    pub fn new<S, B, T>(_service: S, default_namespace: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            default_ns: default_namespace.into(),
            valid_until: None,
        }
    }

    pub fn with_valid_until(self, valid_until: Option<Timestamp>) -> Self {
        Client { valid_until, ..self }
    }

    pub fn valid_until(&self) -> &Option<Timestamp> {
        &self.valid_until
    }

    pub async fn try_default() -> Result<Self> {
        Self::try_from(Config::infer().await.map_err(Error::InferConfig)?)
    }

    pub fn default_namespace(&self) -> &str {
        &self.default_ns
    }
}

impl Client {
    pub async fn apiserver_version(&self) -> Result<k8s_openapi::apimachinery::pkg::version::Info> {
        let result = wit_api::get_api_server_version().await.map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    pub async fn list_api_groups(&self) -> Result<k8s_meta_v1::APIGroupList> {
        let result = wit_api::list_api_version(wit_api::ApiCategory::Named, false)
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    pub async fn list_api_group_resources(&self, apiversion: &str) -> Result<k8s_meta_v1::APIResourceList> {
        let result = wit_api::list_api_resources(wit_api::ApiCategory::Named, apiversion.to_string())
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    pub async fn list_core_api_versions(&self) -> Result<k8s_meta_v1::APIVersions> {
        let result = wit_api::list_api_version(wit_api::ApiCategory::Core, false)
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    pub async fn list_core_api_resources(&self, version: &str) -> Result<k8s_meta_v1::APIResourceList> {
        let result = wit_api::list_api_resources(wit_api::ApiCategory::Core, version.to_string())
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

impl Client {
    pub async fn list_api_groups_aggregated(&self) -> Result<APIGroupDiscoveryList> {
        let result = wit_api::list_api_version(wit_api::ApiCategory::Named, true)
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }

    pub async fn list_core_api_versions_aggregated(&self) -> Result<APIGroupDiscoveryList> {
        let result = wit_api::list_api_version(wit_api::ApiCategory::Core, true)
            .await
            .map_err(Error::Wit)?;

        serde_json::from_str(&result).map_err(|e| {
            tracing::warn!("{}, {:?}", result, e);
            Error::SerdeError(e)
        })
    }
}

impl TryFrom<Config> for Client {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        Ok(ClientBuilder::try_from(config)?.build())
    }
}

