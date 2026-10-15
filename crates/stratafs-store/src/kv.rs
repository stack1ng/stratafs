use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use futures_core::future::BoxFuture;

use crate::error::StoreError;

pub trait KeyValueStore: Send + Sync {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;
    type Observation: Send + 'static;
    type ReadTransaction<'txn>: ReadKeyValueTransaction<Failure = Self::Failure, Observation = Self::Observation>;
    type Transaction<'txn>: KeyValueTransaction<Failure = Self::Failure, Observation = Self::Observation>;

    // transact runs a transaction, returning the result.
    // A panic inside work propagates to the caller of transact.
    // The transaction is not applied, and the store stays usable.
    fn transact<T: Send>(
        &self,
        work: impl for<'t, 'txn> FnMut(
            &'t mut Self::Transaction<'txn>,
        ) -> BoxFuture<'t, Result<T, Self::Failure>>
        + Send,
    ) -> impl Future<Output = Result<T, StoreError<Self::Rejection, Self::Failure>>> + Send;

    // read_transact runs a read-only transaction, returning the result.
    // A panic inside work propagates to the caller of transact.
    // The transaction is not applied, and the store stays usable.
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

    // get_range returns a range of key-value pairs.
    // If an inverted (end < start) or fully excluded (start == end == Bound::Excluded) range is provided, the call panics.
    fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>, Self::Failure>> + Send;

    // observe returns an observation, which can be observed from the store (with changed())
    // to give a future that resolves once this key is in a different state
    // from what this transaction saw, after the transaction finishes.
    // Fails if the transaction does not finish successfully.
    fn observe(&self, key: Bytes) -> Result<Self::Observation, Self::Failure>;
}

#[track_caller]
pub fn assert_valid_range<T: Ord + ?Sized>(range: &impl RangeBounds<T>) {
    match (range.start_bound(), range.end_bound()) {
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) if start > end => panic!("range start is greater than range end"),
        (Bound::Excluded(start), Bound::Excluded(end)) if start == end => {
            panic!("range start and end are equal and excluded")
        }
        _ => {}
    }
}
