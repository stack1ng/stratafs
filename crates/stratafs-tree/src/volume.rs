use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_core::future::BoxFuture;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkingTransaction, VolumeId};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, SetRejection,
    TransactError, TransactionHandle,
};

use crate::concatenated_bytes::ConcatenatedBytes;
use crate::error::TreeFailure;
use crate::header::{TreeVolumeId, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{LeafEntry, Node, NodeHeader, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeShared;

pub struct TreeVolume<S, A, R, const NODE_CAPACITY: usize> {
    shared: Arc<TreeShared<S, A, R, NODE_CAPACITY>>,
    id: TreeVolumeId,
}

pub struct TreeTransaction<'txn, T, S, A, R, const NODE_CAPACITY: usize> {
    pub(crate) inner: TransactionHandle<'txn, T>,
    pub(crate) shared: Arc<TreeShared<S, A, R, NODE_CAPACITY>>,
    pub(crate) volume: TreeVolumeId,
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
            volume: self.id,
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

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: ReadKeyValueTransaction,
{
    pub(crate) async fn get_volume_header(&self) -> Result<VolumeHeader, TreeFailure<T::Failure>> {
        let stored = self
            .inner
            .get(TreeKey::VolumeHeader(self.volume).into())
            .await?
            .ok_or(TreeFailure::Corrupt(CorruptValue::MissingVolumeHeader))?;
        VolumeHeader::view(&stored)
            .copied()
            .map_err(TreeFailure::Corrupt)
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: KeyValueTransaction,
{
    pub(crate) async fn set_volume_header(
        &mut self,
        new_value: VolumeHeader,
    ) -> Result<(), StoreError<SetRejection<T::Rejection>, TreeFailure<T::Failure>>> {
        self.inner
            .set(
                TreeKey::VolumeHeader(self.volume).into(),
                Bytes::from(&new_value),
            )
            .await
            .map_err(with_tree_failure)
    }
}

pub(crate) fn with_tree_failure<R, F>(error: StoreError<R, F>) -> StoreError<R, TreeFailure<F>> {
    match error {
        StoreError::Rejected(rejection) => StoreError::Rejected(rejection),
        StoreError::Failed(failure) => StoreError::Failed(TreeFailure::Store(failure)),
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

    // get_range is implemented by walking the tree. The returned Bytes
    // are all pinned to their containing leaf node's buffer, so for any long lived usage, they should be copied.
    async fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, Self::Failure> {
        let start_target = match range.start_bound() {
            Included(key) => ConcatenatedBytes::from(&key[..]),
            Excluded(key) => ConcatenatedBytes::new(key, &[0]),
            Unbounded => ConcatenatedBytes::default(),
        };

        let end = range.end_bound();
        let reached_end = |key: &[u8]| match end {
            Included(end) => key > &end[..],
            Excluded(end) => key >= &end[..],
            Unbounded => false,
        };

        let mut cursor = self.descend(&start_target).await?;
        let mut entries: Vec<(Bytes, Bytes)> =
            Vec::with_capacity(limit.min(cursor.destination.entries.len()));
        while entries.len() < limit {
            match self.next_entry(&mut cursor).await? {
                None => break,
                Some((entry, leaf_bytes)) => {
                    let (key, value) = entry.key_and_value().map_err(TreeFailure::Corrupt)?;
                    if reached_end(key) {
                        break;
                    }
                    entries.push((leaf_bytes.slice_ref(key), leaf_bytes.slice_ref(value)));
                }
            }
        }

        Ok(entries)
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
        let target = self.descend(&key).await?;
        let mut target_leaf = SlottedList::<BytesMut, _, R>::from(target.destination.entries);

        let insert_at = match target.destination.position {
            Ok(existing_index) => {
                target_leaf.replace(existing_index, &LeafEntry::new(&key, &value));
                self.inner
                    .set(
                        TreeKey::Node(target.destination.id).into(),
                        Node::write(
                            NodeHeader {
                                kind: NodeKind::Leaf,
                                epoch: target.destination.epoch, // todo: wire up COW
                            },
                            target_leaf,
                        ),
                    )
                    .await
                    .map_err(with_tree_failure)?;
                return Ok(());
            }
            Err(insert_at) => insert_at,
        };

        todo!();
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
