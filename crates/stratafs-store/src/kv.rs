use std::ops::Range;

use bytes::Bytes;

use crate::error::StoreError;

pub trait KeyValueStore: Send + Sync {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;
    type ReadTransaction: ReadKeyValueTransaction<Failure = Self::Failure>;
    type Transaction: KeyValueTransaction<Failure = Self::Failure>;

    fn transact<T: Send, Fut: Future<Output = Result<T, Self::Failure>> + Send>(
        &self,
        work: impl FnMut(Self::Transaction) -> Fut + Send,
    ) -> impl Future<Output = Result<T, StoreError<Self::Rejection, Self::Failure>>> + Send;

    fn read_transact<T: Send, Fut: Future<Output = Result<T, Self::Failure>> + Send>(
        &self,
        work: impl FnMut(Self::ReadTransaction) -> Fut + Send,
    ) -> impl Future<Output = Result<T, StoreError<Self::Rejection, Self::Failure>>> + Send;
}

pub trait KeyValueTransaction: ReadKeyValueTransaction {
    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Failure>;

    fn clear(&self, key: Bytes) -> Result<(), Self::Failure>;
}

pub trait ReadKeyValueTransaction: Send + Sync {
    type Failure: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: Bytes) -> impl Future<Output = Result<Option<Bytes>, Self::Failure>> + Send;

    fn get_range(
        &self,
        range: Range<Bytes>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>, Self::Failure>> + Send;

    fn watch(&self, key: Bytes)
    -> impl Future<Output = Result<(), Self::Failure>> + Send + 'static;
}
