use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use futures_core::future::BoxFuture;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::ForkingTransaction;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, SetRejection,
    TransactError, TransactionHandle, assert_valid_range,
};
use zerocopy::KnownLayout;

use crate::branch_observation::BranchWatch;
use crate::concatenated_bytes::ConcatenatedBytes;
use crate::error::TreeFailure;
use crate::header::{TreeVolumeId, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry};
use crate::serialized::CorruptValue;
use crate::slotted::SlottedList;
use crate::tree::{TreeConfig, TreeConfigLimits, TreeShared};

pub struct TreeVolume<S, A, C> {
    pub(crate) shared: Arc<TreeShared<S, A, C>>,
    pub(crate) id: TreeVolumeId,
}

pub struct TreeTransaction<'txn, T, S, A, C> {
    pub(crate) inner: TransactionHandle<'txn, T>,
    pub(crate) shared: Arc<TreeShared<S, A, C>>,
    pub(crate) volume: TreeVolumeId,
}

impl<S, A, C> TreeVolume<S, A, C> {
    fn transaction<'txn, T>(
        &self,
        inner: TransactionHandle<'txn, T>,
    ) -> TreeTransaction<'txn, T, S, A, C> {
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

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
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

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
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

fn refused_write_as_failure<F, R>(
    error: StoreError<SetRejection<R>, TreeFailure<F, R>>,
) -> TreeFailure<F, R> {
    match error {
        StoreError::Failed(failure) => failure,
        StoreError::Rejected(rejection) => TreeFailure::RefusedWrite(rejection),
    }
}

impl<S, A, C> KeyValueStore for TreeVolume<S, A, C>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    type Rejection = S::Rejection;
    type Failure = TreeFailure<S::Failure, S::Rejection>;
    type Observation = BranchWatch<S::Observation>;
    type ReadTransaction<'txn> = TreeTransaction<'txn, S::ReadTransaction<'txn>, S, A, C>;
    type Transaction<'txn> = TreeTransaction<'txn, S::Transaction<'txn>, S, A, C>;

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
        self.branch_changed(observation).await
    }
}

impl<T, S, A, C> ReadKeyValueTransaction for TreeTransaction<'_, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    A: Send + Sync,
    C: TreeConfig,
{
    type Failure = TreeFailure<T::Failure, S::Rejection>;
    type Observation = BranchWatch<T::Observation>;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        let descent = self.descend(&key).await?;
        Ok(descent
            .destination
            .found_value()
            .map_err(TreeFailure::Corrupt)?
            .map(|value| descent.destination.entries.get_ref().slice_ref(value)))
    }

    // get_range is implemented by walking the tree. The returned Bytes
    // are all pinned to their containing leaf node's buffer, so for any long lived usage, they should be copied.
    async fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, Self::Failure> {
        assert_valid_range(&range);
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

    async fn observe(&self, key: Bytes) -> Result<Self::Observation, Self::Failure> {
        let descent = self.descend(&key).await?;
        self.observe_branch(key, &descent).await
    }
}

impl<T, S, A, C> KeyValueTransaction for TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    type Rejection = T::Rejection;

    async fn set(
        &mut self,
        key: Bytes,
        value: impl Buf + Send,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        let fits_as_node_entry =
            |length: Option<usize>| length.is_some_and(|length| length <= C::LARGEST_ENTRY_SIZE);
        if !fits_as_node_entry(
            key.len()
                .checked_add(value.remaining())
                .and_then(LeafEntry::<C::Range>::size_for_metadata),
        ) || !fits_as_node_entry(InternalEntry::size_for_metadata(key.len()))
        {
            return Err(StoreError::Rejected(SetRejection::TooLarge));
        }
        let target = self.descend(&key).await?;
        let mut target_leaf =
            SlottedList::<BytesMut, _, C::Range>::from(target.destination.entries);
        match target.destination.position {
            Ok(index) => target_leaf.replace_entry(index, &key, value),
            Err(insert_at) => target_leaf.insert_entry(insert_at, &key, value),
        }

        self.write_over(
            target.volume,
            target.steps,
            target.destination.node,
            target_leaf,
        )
        .await
    }

    async fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        let target = self.descend(&key).await?;
        let mut target_leaf =
            SlottedList::<BytesMut, _, C::Range>::from(target.destination.entries);
        match target.destination.position {
            Ok(index) => target_leaf.remove(index),
            Err(_) => return Ok(()),
        }

        self.write_over(
            target.volume,
            target.steps,
            target.destination.node,
            target_leaf,
        )
        .await
        .map_err(refused_write_as_failure)
    }
}

impl<T, S, A, C> ForkingTransaction for TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    type VolumeId = TreeVolumeId;

    async fn fork(&mut self) -> Result<TreeVolumeId, Self::Failure> {
        let mut from_header = self.get_volume_header().await?;
        from_header.epoch += 1;
        self.set_volume_header(from_header)
            .await
            .map_err(refused_write_as_failure)?;

        let new_volume = TreeVolumeId((self.shared.id_allocator)());
        self.inner
            .set(
                TreeKey::VolumeHeader(new_volume).into(),
                Bytes::from(&from_header),
            )
            .await
            .map_err(with_tree_failure)
            .map_err(refused_write_as_failure)?;

        Ok(new_volume)
    }
}
