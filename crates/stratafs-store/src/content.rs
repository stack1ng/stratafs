use bytes::Bytes;
use thiserror::Error;

use crate::error::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

#[derive(Debug, Error)]
#[error("content not found")]
pub struct UnknownContent;

#[derive(Debug, Error)]
pub enum GetRejection {
    #[error(transparent)]
    UnknownContent(UnknownContent),
    #[error("requested range is beyond the content length {len}")]
    OutOfBounds { len: u64 },
}

pub trait ContentStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn put(&self, bytes: Bytes) -> impl Future<Output = Result<ContentHash, Self::Error>> + Send;

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Bytes, StoreError<GetRejection, Self::Error>>> + Send;
}
