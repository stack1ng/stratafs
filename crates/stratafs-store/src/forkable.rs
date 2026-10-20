use std::fmt::Debug;
use std::hash::Hash;

use bytes::Bytes;

use crate::error::StoreError;
use crate::kv::{KeyValueStore, KeyValueTransaction};

#[derive(Debug, thiserror::Error)]
#[error("the volume does not exist")]
pub struct UnknownVolume;

pub trait ForkableKeyValueStore: Send + Sync + 'static {
    type Failure: std::error::Error + Send + Sync + 'static;
    type CreateError: std::error::Error + Send + Sync + 'static;
    type VolumeId: Clone
        + Debug
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + Into<Bytes>
        + TryFrom<Bytes, Error: std::error::Error + Send + Sync + 'static>;
    type Volume: for<'txn> KeyValueStore<
            Failure = Self::Failure,
            Transaction<'txn>: ForkingTransaction<VolumeId = Self::VolumeId>,
        >;

    fn create(&self) -> impl Future<Output = Result<Self::VolumeId, Self::CreateError>> + Send;

    fn open(
        &self,
        id: Self::VolumeId,
    ) -> impl Future<Output = Result<Self::Volume, StoreError<UnknownVolume, Self::Failure>>> + Send;
}

pub trait ForkingTransaction: KeyValueTransaction {
    type VolumeId;

    // fork creates a new volume that is a copy of the current volume,
    // at its current state in the transaction.
    // The new volume exists once the transaction commits.
    fn fork(&mut self) -> impl Future<Output = Result<Self::VolumeId, Self::Failure>> + Send;
}
