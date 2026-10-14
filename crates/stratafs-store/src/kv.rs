use std::ops::RangeBounds;

use bytes::Bytes;
use futures_core::future::BoxFuture;

use crate::error::StoreError;

pub trait KeyValueStore: Send + Sync {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;
    type Observation: Send + 'static;
    type ReadTransaction<'txn>: ReadKeyValueTransaction<Failure = Self::Failure, Observation = Self::Observation>;
    type Transaction<'txn>: KeyValueTransaction<Failure = Self::Failure, Observation = Self::Observation>;

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

    fn changed(
        &self,
        observation: Self::Observation,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
}

pub trait KeyValueTransaction: ReadKeyValueTransaction {
    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Failure>;

    fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure>;
}

pub trait ReadKeyValueTransaction: Send + Sync {
    type Failure: std::error::Error + Send + Sync + 'static;
    type Observation: Send + 'static;

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

    // observe returns an observation, which can be awaited from the store (with changed())
    // to give a future that resolves once this key is in a different state
    // from what this transaction saw, after the transaction finishes.
    // Fails if the transaction does not finish successfully.
    fn observe(&self, key: Bytes) -> Result<Self::Observation, Self::Failure>;
}
