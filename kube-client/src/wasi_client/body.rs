use bytes::Bytes;
use std::fmt;

#[derive(Clone)]
pub struct Body {
    bytes: Bytes,
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body").finish()
    }
}

impl Body {
    pub const fn empty() -> Self {
        Self { bytes: Bytes::new() }
    }

    pub async fn collect_bytes(self) -> Result<Bytes, crate::Error> {
        Ok(self.bytes)
    }

    pub fn try_clone(&self) -> Option<Self> {
        Some(self.clone())
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl From<Vec<u8>> for Body {
    fn from(vec: Vec<u8>) -> Self {
        Self { bytes: Bytes::from(vec) }
    }
}
