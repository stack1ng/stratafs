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
use zerocopy::KnownLayout;

use crate::concatenated_bytes::ConcatenatedBytes;
use crate::error::TreeFailure;
use crate::header::{TreeVolumeId, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeShared};

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
) -> TransactError<R, TreeFailure<F, R>, E> {
    match error {
        TransactError::Rejected(rejection) => TransactError::Rejected(rejection),
        TransactError::Failed(failure) => TransactError::Failed(TreeFailure::Store(failure)),
        TransactError::Aborted(error) => TransactError::Aborted(error),
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
{
    pub(crate) async fn get_volume_header(
        &self,
    ) -> Result<VolumeHeader, TreeFailure<T::Failure, S::Rejection>> {
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
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
{
    pub(crate) async fn set_volume_header(
        &mut self,
        new_value: VolumeHeader,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        self.inner
            .set(
                TreeKey::VolumeHeader(self.volume).into(),
                Bytes::from(&new_value),
            )
            .await
            .map_err(with_tree_failure)
    }
}

pub(crate) fn with_tree_failure<R, F>(
    error: StoreError<SetRejection<R>, F>,
) -> StoreError<SetRejection<R>, TreeFailure<F, R>> {
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
    type Failure = TreeFailure<S::Failure, S::Rejection>;
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
    S: KeyValueStore,
    A: Send + Sync,
    R: AddressableRange,
{
    type Failure = TreeFailure<T::Failure, S::Rejection>;
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
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    type Rejection = T::Rejection;

    async fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        let fits_as_node_entry = |length: Option<usize>| {
            length
                .is_some_and(|length| length <= TreeConfig::<R, NODE_CAPACITY>::LARGEST_ENTRY_SIZE)
        };
        if !fits_as_node_entry(LeafEntry::<R>::size_for_metadata(key.len() + value.len()))
            || !fits_as_node_entry(InternalEntry::size_for_metadata(key.len()))
        {
            return Err(StoreError::Rejected(SetRejection::TooLarge));
        }
        let Some(new_entry) = LeafEntry::<R>::new(&key, &value) else {
            return Err(StoreError::Rejected(SetRejection::TooLarge));
        };
        let target = self.descend(&key).await?;
        let mut target_leaf = SlottedList::<BytesMut, _, R>::from(target.destination.entries);
        match target.destination.position {
            Ok(index) => target_leaf.replace(index, &new_entry),
            Err(insert_at) => target_leaf.insert(insert_at, &new_entry),
        }

        self.write_over(
            target.steps,
            target.destination.id,
            target.destination.epoch,
            NodeKind::Leaf,
            target_leaf,
        )
        .await
    }

    async fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        let target = self.descend(&key).await?;
        let mut target_leaf = SlottedList::<BytesMut, _, R>::from(target.destination.entries);
        match target.destination.position {
            Ok(index) => target_leaf.remove(index),
            Err(_) => return Ok(()),
        }

        self.write_over(
            target.steps,
            target.destination.id,
            target.destination.epoch,
            NodeKind::Leaf,
            target_leaf,
        )
        .await
        .map_err(|error| match error {
            StoreError::Failed(failure) => failure,
            StoreError::Rejected(rejection) => TreeFailure::RefusedWrite(rejection),
        })
    }
}

impl<T, S, A, R, const NODE_CAPACITY: usize> ForkingTransaction
    for TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    async fn fork(&mut self) -> Result<VolumeId, Self::Failure> {
        todo!()
    }
}
