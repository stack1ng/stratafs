use std::marker::PhantomData;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;

use crate::error::StoreError;

#[derive(Debug, thiserror::Error)]
#[error("the transaction that made this observation did not commit")]
pub struct NotCommitted;

#[derive(Debug, thiserror::Error)]
pub enum TransactError<R, F, E> {
    // The store refused the transaction, and it was not applied.
    // the exact reasons for this are specific to the store implementation.
    #[error(transparent)]
    Rejected(R),

    // The transaction failed due to some backend failure, it may or may not have been applied.
    #[error(transparent)]
    Failed(F),

    // work returned an error, so the transaction was not applied.
    #[error(transparent)]
    Aborted(E),
}

pub trait KeyValueStore: Send + Sync + 'static {
    type Rejection: std::error::Error + Send + Sync + 'static;
    type Failure: std::error::Error + Send + Sync + 'static;
    type Observation: Send + 'static;
    type ReadTransaction<'txn>: ReadKeyValueTransaction<Failure = Self::Failure, Observation = Self::Observation>;
    type Transaction<'txn>: KeyValueTransaction<
            Rejection = Self::Rejection,
            Failure = Self::Failure,
            Observation = Self::Observation,
        >;

    const LARGEST_ACCEPTED_ENTRY: usize;

    // transact runs a transaction, returning the result.
    // If work returns an error, the transaction is not applied, and transact returns that error as Aborted.
    // A panic inside work propagates to the caller of transact.
    // The transaction is not applied, and the store stays usable.
    fn transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<Self::Rejection, Self::Failure, E>>> + Send;

    // read_transact runs a read-only transaction, returning the result.
    // If work returns an error, read_transact returns that error as Aborted.
    // A panic inside work propagates to the caller of read_transact.
    // The transaction is not applied, and the store stays usable.
    fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<Self::Rejection, Self::Failure, E>>> + Send;

    // changed returns a future that resolves once the observed key differs from what its transaction saw (see observe).
    // It fails if the transaction that produced the observation did not finish successfully.
    fn changed(
        &self,
        observation: Self::Observation,
    ) -> impl Future<Output = Result<(), StoreError<NotCommitted, Self::Failure>>> + Send;
}

// TransactionHandle gives work its transaction by value, so a store can wrap another store's transaction in its own without allocating.
// It exists only to carry two lifetimes: the transaction's, which Rust ignores inside Self::Transaction<'txn>,
// and the store's, which outlives the transaction, so a wrapping store can lend its own fields to the transaction instead of cloning them.
pub struct TransactionHandle<'txn, 'store, T> {
    transaction: T,
    lifetime: PhantomData<&'txn &'store ()>,
}

impl<T> TransactionHandle<'_, '_, T> {
    pub fn new(transaction: T) -> Self {
        Self {
            transaction,
            lifetime: PhantomData,
        }
    }
}

impl<T> Deref for TransactionHandle<'_, '_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.transaction
    }
}

impl<T> DerefMut for TransactionHandle<'_, '_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.transaction
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SetRejection<R> {
    #[error("the entry is larger than this store accepts")]
    TooLarge,
    #[error(transparent)]
    Store(R),
}

pub trait KeyValueTransaction: ReadKeyValueTransaction {
    type Rejection: std::error::Error + Send + Sync + 'static;

    fn set(
        &mut self,
        key: Bytes,
        value: impl Buf + Send,
    ) -> impl Future<Output = Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>>> + Send;

    fn clear(&mut self, key: Bytes) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        async move { self.clear_range(&key..=&key).await }
    }

    // clear_range removes every key in a range.
    // If an inverted (end < start) or fully excluded (start == end == Bound::Excluded) range is provided, the call panics.
    fn clear_range(
        &mut self,
        range: impl RangeBounds<Bytes> + Send,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
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
    fn observe(
        &self,
        key: Bytes,
    ) -> impl Future<Output = Result<Self::Observation, Self::Failure>> + Send;
}

pub fn is_valid_range<T: Ord + ?Sized>(range: &impl RangeBounds<T>) -> bool {
    invalid_range_reason(range).is_none()
}

#[track_caller]
pub fn assert_valid_range<T: Ord + ?Sized>(range: &impl RangeBounds<T>) {
    if let Some(reason) = invalid_range_reason(range) {
        panic!("{reason}");
    }
}

fn invalid_range_reason<T: Ord + ?Sized>(range: &impl RangeBounds<T>) -> Option<&'static str> {
    match (range.start_bound(), range.end_bound()) {
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) if start > end => Some("range start is greater than range end"),
        (Bound::Excluded(start), Bound::Excluded(end)) if start == end => {
            Some("range start and end are equal and excluded")
        }
        _ => None,
    }
}
