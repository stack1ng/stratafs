use bytes::Bytes;

use crate::error::StoreError;
use crate::kv::{KeyValueStore, KeyValueTransaction};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VolumeId(pub Bytes);

#[derive(Debug, thiserror::Error)]
#[error("the volume does not exist")]
pub struct UnknownVolume;

pub trait ForkableKeyValueStore: Send + Sync + 'static {
    type Failure: std::error::Error + Send + Sync + 'static;
    type Volume: for<'txn> KeyValueStore<Failure = Self::Failure, Transaction<'txn>: ForkingTransaction>;

    fn create(&self) -> impl Future<Output = Result<VolumeId, Self::Failure>> + Send;

    fn open(
        &self,
        id: VolumeId,
    ) -> impl Future<Output = Result<Self::Volume, StoreError<UnknownVolume, Self::Failure>>> + Send;
}

pub trait ForkingTransaction: KeyValueTransaction {
    // fork creates a new volume that is a complete copy of the current volume
    fn fork(&mut self) -> impl Future<Output = Result<VolumeId, Self::Failure>> + Send;
}
