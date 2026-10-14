use std::ops::RangeBounds;

use bytes::Bytes;
use futures_core::future::BoxFuture;

use crate::error::StoreError;

pub trait KeyValueStore: Send + Sync {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;
    type ReadTransaction<'txn>: ReadKeyValueTransaction<Failure = Self::Failure>;
    type Transaction<'txn>: KeyValueTransaction<Failure = Self::Failure>;

    fn transact<T: Send>(
        &self,
        work: impl for<'t, 'txn> FnMut(
            &'t mut Self::Transaction<'txn>,
        ) -> BoxFuture<'t, Result<T, Self::Failure>>
        + Send,
    ) -> impl Future<Output = Result<T, StoreError<Self::Rejection, Self::Failure>>> + Send;

    fn read_transact<T: Send>(
        &self,
        work: impl for<'t, 'txn> FnMut(
            &'t Self::ReadTransaction<'txn>,
        ) -> BoxFuture<'t, Result<T, Self::Failure>>
        + Send,
    ) -> impl Future<Output = Result<T, StoreError<Self::Rejection, Self::Failure>>> + Send;
}

pub trait KeyValueTransaction: ReadKeyValueTransaction {
    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Failure>;

    fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure>;
}

pub trait ReadKeyValueTransaction: Send + Sync {
    type Failure: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: Bytes) -> impl Future<Output = Result<Option<Bytes>, Self::Failure>> + Send {
        async move {
            let found = self.get_range(&key..=&key, 1).await?;
            Ok(found.into_iter().next().map(|(_, value)| value))
        }
    }

    fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>, Self::Failure>> + Send;

    // watch returns a future that resolves once this key is in a different state
    // from what this transaction saw, after the transaction finishes.
    // Fails if the transaction does not finish successfully.
    fn watch(&self, key: Bytes)
    -> impl Future<Output = Result<(), Self::Failure>> + Send + 'static;
}
