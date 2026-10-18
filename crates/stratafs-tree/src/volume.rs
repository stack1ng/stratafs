use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use futures_core::future::BoxFuture;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkingTransaction, VolumeId};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, SetRejection,
    TransactError, TransactionHandle,
};

use crate::error::TreeFailure;
use crate::slotted::AddressableRange;
use crate::tree::TreeShared;

pub struct TreeVolume<S, A, R, const NODE_CAPACITY: usize> {
    shared: Arc<TreeShared<S, A, R, NODE_CAPACITY>>,
    id: VolumeId,
}

pub struct TreeTransaction<'txn, T, S, A, R, const NODE_CAPACITY: usize> {
    inner: TransactionHandle<'txn, T>,
    shared: Arc<TreeShared<S, A, R, NODE_CAPACITY>>,
    volume: VolumeId,
}

pub struct TreeObservation;

impl<S, A, R, const NODE_CAPACITY: usize> TreeVolume<S, A, R, NODE_CAPACITY> {
    fn transaction<'txn, T>(
        &self,
        inner: TransactionHandle<'txn, T>,
    ) -> TreeTransaction<'txn, T, S, A, R, NODE_CAPACITY> {
        TreeTransaction {
            inner,
            shared: Arc::clone(&self.shared),
            volume: self.id.clone(),
        }
    }
}

fn store_failure_as_tree_failure<R, F, E>(
    error: TransactError<R, F, E>,
) -> TransactError<R, TreeFailure<F>, E> {
    match error {
        TransactError::Rejected(rejection) => TransactError::Rejected(rejection),
        TransactError::Failed(failure) => TransactError::Failed(TreeFailure::Store(failure)),
        TransactError::Aborted(error) => TransactError::Aborted(error),
    }
}

impl<S, A, R, const NODE_CAPACITY: usize> KeyValueStore for TreeVolume<S, A, R, NODE_CAPACITY>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    type Rejection = S::Rejection;
    type Failure = TreeFailure<S::Failure>;
    type Observation = TreeObservation;
    type ReadTransaction<'txn> =
        TreeTransaction<'txn, S::ReadTransaction<'txn>, S, A, R, NODE_CAPACITY>;
    type Transaction<'txn> = TreeTransaction<'txn, S::Transaction<'txn>, S, A, R, NODE_CAPACITY>;

    async fn transact<T: Send, E: Send>(
        &self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, Self::Transaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.shared
            .store
            .transact(|inner| work(TransactionHandle::new(self.transaction(inner))))
            .await
            .map_err(store_failure_as_tree_failure)
    }

    async fn read_transact<T: Send, E: Send>(
        &self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.shared
            .store
            .read_transact(|inner| work(TransactionHandle::new(self.transaction(inner))))
            .await
            .map_err(store_failure_as_tree_failure)
    }

    async fn changed(
        &self,
        observation: Self::Observation,
    ) -> Result<(), StoreError<NotCommitted, Self::Failure>> {
        todo!()
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> ReadKeyValueTransaction
    for TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: ReadKeyValueTransaction,
    S: Send + Sync,
    A: Send + Sync,
    R: AddressableRange,
{
    type Failure = TreeFailure<T::Failure>;
    type Observation = TreeObservation;

    async fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, Self::Failure> {
        todo!()
    }

    fn observe(&self, key: Bytes) -> Result<Self::Observation, Self::Failure> {
        todo!()
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> KeyValueTransaction
    for TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: KeyValueTransaction,
    S: Send + Sync,
    A: Send + Sync,
    R: AddressableRange,
{
    type Rejection = T::Rejection;

    async fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        todo!()
    }

    async fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        todo!()
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> ForkingTransaction
    for TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: KeyValueTransaction,
    S: Send + Sync,
    A: Send + Sync,
    R: AddressableRange,
{
    async fn fork(&mut self) -> Result<VolumeId, Self::Failure> {
        todo!()
    }
}
