pub mod operations;

use std::fmt;

use bytes::Bytes;
use futures_core::Stream;
use thiserror::Error;
use vec1::Vec1;

use crate::content::ContentHash;
use crate::error::StoreError;
use crate::metadata::operations::{Op, Operations, Outcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(pub u64);

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VolumeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Key,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    // opaque metadata
    SetKey(KeyValue),
    DeleteKey {
        key: Key,
    },
    // some data that was written to a file, + the location of the change in the file
    Write {
        file: FileId,
        offset: u64,
        size: u64,
        content: ContentHash,
        content_offset: u64,
    },
    // the new length that the given file was truncated to
    Truncate {
        file: FileId,
        len: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleBlob {
    pub offset: u64,
    pub size: u64,
    pub content: ContentHash,
    pub content_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileContent {
    pub info: FileInfo,
    pub blobs: Vec<VisibleBlob>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileInfo {
    pub len: u64,
    pub seq: Option<Seq>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored<E> {
    pub seq: Seq,
    pub entry: E,
}

#[derive(Debug, Error)]
#[error("volume not found")]
pub struct UnknownVolume;

#[derive(Debug, Error)]
pub enum ForkRejection {
    #[error(transparent)]
    UnknownVolume(UnknownVolume),
    #[error("fork position {at} is beyond head {head}")]
    BeyondHead { at: Seq, head: Seq },
}

#[derive(Debug, Error)]
pub enum TransactRejection<R> {
    #[error(transparent)]
    UnknownVolume(UnknownVolume),
    #[error("assertion at operation {index} failed")]
    AssertionFailed { index: usize },
    #[error(transparent)]
    Store(R),
}

pub type TransactError<R, F> = StoreError<TransactRejection<R>, F>;

pub trait MetadataStore: Send + Sync {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;

    // create a new volume, bound to nothing
    fn create(&self) -> impl Future<Output = Result<VolumeId, Self::Failure>> + Send;

    // create a new volume that has all entries in the given parent volume,
    // up to and including the given sequence. Then after that point, all entries are exclusive to the new volume.
    fn fork(
        &self,
        parent_volume: &VolumeId,
        at: Seq,
    ) -> impl Future<Output = Result<VolumeId, StoreError<ForkRejection, Self::Failure>>> + Send;

    fn execute(
        &self,
        volume: &VolumeId,
        operations: Vec1<Op>,
    ) -> impl Future<Output = Result<Vec1<Outcome>, TransactError<Self::Rejection, Self::Failure>>> + Send;

    fn transact<T>(
        &self,
        volume: &VolumeId,
        operations: T,
    ) -> impl Future<Output = Result<T::Output, TransactError<Self::Rejection, Self::Failure>>> + Send
    where
        T: Operations + Send,
        T::Layout: Send,
    {
        async move {
            let mut ops = Vec::new();
            let layout = operations.into_ops(&mut ops);
            let Ok(ops) = Vec1::try_from_vec(ops) else {
                panic!("Operations::into_ops produced no operations");
            };
            let mut outcomes = self.execute(volume, ops).await?.into_iter();
            match (T::from_outcomes(layout, &mut outcomes), outcomes.next()) {
                (Some(output), None) => Ok(output),
                _ => panic!(
                    "MetadataStore::execute returned outcomes that do not match its operations"
                ),
            }
        }
    }

    // get a stream of the entries in the given volume, starting after the given sequence
    fn entries(
        &self,
        volume: VolumeId,
        after: Seq,
    ) -> impl Stream<Item = Result<Stored<Entry>, StoreError<UnknownVolume, Self::Failure>>>
    + Send
    + 'static;

    // get a stream of the metadata entries whose keys start with the given prefix, in key order.
    // This is not a snapshot: every key that exists the whole time the stream is being read
    // is returned exactly once, but keys created or deleted during that time may or may not appear.
    fn list_keys(
        &self,
        volume: VolumeId,
        prefix: Key,
    ) -> impl Stream<Item = Result<Stored<KeyValue>, StoreError<UnknownVolume, Self::Failure>>>
    + Send
    + 'static;

    // get a stream of the head, as it changes in realtime
    fn watch(
        &self,
        volume: VolumeId,
        after: Seq,
    ) -> impl Stream<Item = Result<Seq, StoreError<UnknownVolume, Self::Failure>>> + Send + 'static;
}
