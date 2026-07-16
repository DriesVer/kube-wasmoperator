use crate::Config;

/// Extensions to [`Config`](crate::Config) for custom [`Client`](crate::Client).
pub trait ConfigExt: private::Sealed {}

impl ConfigExt for Config {}

mod private {
    pub trait Sealed {}
    impl Sealed for crate::Config {}
}
