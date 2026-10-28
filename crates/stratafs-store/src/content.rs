use bytes::Buf;
use thiserror::Error;
use xxhash_rust::xxh3::{Xxh3, xxh3_128};

use crate::durability::DurableAgainst;
use crate::error::StoreError;

// ContentHash is content's 128 bit xxh3 hash, which names it in a content store. Different content with the same
// hash would be taken for the same content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentHash(pub [u8; 16]);

impl ContentHash {
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(xxh3_128(bytes).to_be_bytes())
    }
}

// ContentHasher makes the ContentHash of content given in pieces.
#[derive(Default)]
pub struct ContentHasher(Xxh3);

impl ContentHasher {
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    #[must_use]
    pub fn finish(&self) -> ContentHash {
        ContentHash(self.0.digest128().to_be_bytes())
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
    type Content: Buf + Send;

    fn put(
        &self,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<ContentHash, Self::Failure>> + Send;

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Self::Content, StoreError<GetRejection, Self::Failure>>> + Send;

    // ensure_durable makes every put that finished before it is called durable against a failure, and resolves
    // once they are.
    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
}
