use bytes::Buf;
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
    type Failure: std::error::Error + Send + Sync + 'static;

    fn put(
        &self,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<ContentHash, Self::Failure>> + Send;

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<impl Buf + Send, StoreError<GetRejection, Self::Failure>>> + Send;
}
