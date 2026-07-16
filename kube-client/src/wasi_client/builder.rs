use jiff::Timestamp;
use crate::{Client, Config, Error, Result};

pub struct ClientBuilder {
    default_ns: String,
    valid_until: Option<Timestamp>,
}

impl ClientBuilder {
    pub fn new(default_namespace: impl Into<String>) -> Self {
        Self {
            default_ns: default_namespace.into(),
            valid_until: None,
        }
    }

    pub fn with_layer<L>(self, _layer: &L) -> Self {
        self
    }

    pub fn with_valid_until(mut self, valid_until: Option<Timestamp>) -> Self {
        self.valid_until = valid_until;
        self
    }

    pub fn build(self) -> Client {
        Client::new_raw(self.default_ns, self.valid_until)
    }
}

impl TryFrom<Config> for ClientBuilder {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        Ok(Self {
            default_ns: config.default_namespace,
            valid_until: None,
        })
    }
}
