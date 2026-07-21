use crate::Result;

/// Configuration object for accessing a Kubernetes cluster under WASI.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// The configured default namespace
    pub default_namespace: String,
}

impl Config {
    /// Construct a new config with a given default namespace.
    pub fn new(default_namespace: String) -> Self {
        Self { default_namespace }
    }

    /// Infer a Kubernetes client configuration in WASI environments.
    pub async fn infer() -> Result<Self, InferConfigError> {
        if let Ok(ns) = std::env::var("KUBE_NAMESPACE") {
            if !ns.is_empty() {
                return Ok(Self {
                    default_namespace: ns,
                });
            }
        }

        if let Ok(ns) = std::env::var("POD_NAMESPACE") {
            if !ns.is_empty() {
                return Ok(Self {
                    default_namespace: ns,
                });
            }
        }

        match crate::wit_api::get_default_namespace() {
            Ok(ns) => {
                if !ns.is_empty() {
                    return Ok(Self {
                        default_namespace: ns,
                    });
                }
            }
            Err(e) => {
                tracing::debug!("Failed to query default namespace from WIT host: {:?}", e);
            }
        }

        Ok(Self {
            default_namespace: "default".to_string(),
        })
    }
}

/// Failed to infer configuration under WASI.
#[derive(thiserror::Error, Debug)]
#[error("failed to infer config under WASI: {0}")]
pub struct InferConfigError(#[source] pub Box<crate::Error>);

impl From<crate::Error> for InferConfigError {
    fn from(e: crate::Error) -> Self {
        Self(Box::new(e))
    }
}
