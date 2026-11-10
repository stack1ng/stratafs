use bytes::Buf;
use thiserror::Error;

use crate::durability::DurableAgainst;
use crate::error::StoreError;

// ContentId names a blob in a content store. Each put is given one no other put was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentId(pub u128);

// ContentReference is a stored entry's reference to a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReference {
    // New is the reference written with the blob.
    New(ContentId),
    // Existing is a reference written to a blob that already had one.
    Existing(ContentId),
}

impl ContentReference {
    #[must_use]
    pub fn id(self) -> ContentId {
        match self {
            ContentReference::New(id) | ContentReference::Existing(id) => id,
        }
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

    // put keeps content under id, a fresh id no other put was given. The content is kept whether or not a metadata
    // transaction that was running meanwhile commits.
    fn put(
        &self,
        id: &ContentId,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;

    fn get(
        &self,
        id: &ContentId,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Self::Content, StoreError<GetRejection, Self::Failure>>> + Send;

    // delete removes the content kept under id, and succeeds when there is none.
    fn delete(&self, id: &ContentId) -> impl Future<Output = Result<(), Self::Failure>> + Send;

    // ensure_durable makes every put that finished before it is called durable against a failure, and resolves
    // once they are.
    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
}
