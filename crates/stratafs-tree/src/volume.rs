use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use smallvec::SmallVec;
use stratafs_range_bounds_map::RangeBoundsMap;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::ForkingTransaction;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle, assert_valid_range,
};
use zerocopy::KnownLayout;

use crate::concatenated_bytes::ConcatenatedBytes;
use crate::error::TreeFailure;
use crate::header::{NodeId, TreeVolumeId, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry};
use crate::serialized::CorruptValue;
use crate::tree::{TreeConfig, TreeConfigLimits, TreeShared};

pub struct TreeVolume<S, A, C> {
    pub(crate) shared: Arc<TreeShared<S, A, C>>,
    pub(crate) id: TreeVolumeId,
}

pub struct TreeTransaction<'txn, T, S, A, C> {
    pub(crate) inner: TransactionHandle<'txn, 'txn, T>,
    pub(crate) shared: &'txn TreeShared<S, A, C>,
    pub(crate) volume: TreeVolumeId,
    pub(crate) buffered_delta: RangeBoundsMap<
        Bytes,
        // None here means the bytes were cleared
        Option<Bytes>,
    >,
}

// FlushingWork is the error of a transaction's work followed by its flush.
enum FlushingWork<E, F, R> {
    Work(E),
    Flush(TreeFailure<F, R>),
}

impl<S, A, C> TreeVolume<S, A, C> {
    fn transaction<'txn, T>(
        &'txn self,
        inner: TransactionHandle<'txn, 'txn, T>,
    ) -> TreeTransaction<'txn, T, S, A, C> {
        TreeTransaction {
            inner,
            shared: &self.shared,
            volume: self.id,
            buffered_delta: RangeBoundsMap::new(),
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
            .map_err(|corrupt| TreeFailure::Corrupt(corrupt.into()))
    }
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
{
    pub(crate) fn set_volume_header(
        &mut self,
        new_value: VolumeHeader,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        self.inner
            .set(
                TreeKey::VolumeHeader(self.volume).into(),
                Bytes::from(&new_value),
            )
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
    type ReadTransaction<'txn> = TreeTransaction<'txn, S::ReadTransaction<'txn>, S, A, C>;
    type Transaction<'txn> = TreeTransaction<'txn, S::Transaction<'txn>, S, A, C>;

    const LARGEST_ACCEPTED_ENTRY: usize = C::LARGEST_ENTRY_SIZE.saturating_sub(size_of::<NodeId>());

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.shared
            .store
            .transact(|inner| {
                let working = work(TransactionHandle::new(self.transaction(inner)));
                Box::pin(async move {
                    let (result, mut transaction) = working.await.map_err(FlushingWork::Work)?;
                    transaction
                        .flush()
                        .await
                        .map_err(|error| FlushingWork::Flush(refused_write_as_failure(error)))?;
                    let inner = transaction.into_inner().inner.into_inner();
                    Ok((result, TransactionHandle::new(inner)))
                })
            })
            .await
            .map_err(|error| match error {
                TransactError::Rejected(rejection) => TransactError::Rejected(rejection),
                TransactError::Failed(failure) => {
                    TransactError::Failed(TreeFailure::Store(failure))
                }
                TransactError::Aborted(FlushingWork::Work(error)) => TransactError::Aborted(error),
                TransactError::Aborted(FlushingWork::Flush(failure)) => {
                    TransactError::Failed(failure)
                }
            })
    }

    async fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.shared
            .store
            .read_transact(|inner| work(TransactionHandle::new(self.transaction(inner))))
            .await
            .map_err(store_failure_as_tree_failure)
    }

    async fn changed(&self, key: Bytes, seen: Option<Bytes>) -> Result<(), Self::Failure> {
        self.branch_changed(key, seen).await
    }
}

impl<'txn, T, S, A, C> ReadKeyValueTransaction for TreeTransaction<'txn, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    A: Send + Sync,
    C: TreeConfig,
{
    type Failure = TreeFailure<T::Failure, S::Rejection>;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        if let Some(buffered) = self.buffered_delta.get(&key) {
            return Ok(buffered.clone());
        }

        let descent = self.descend(&key).await?;
        Ok(descent
            .destination
            .found_value()
            .map_err(TreeFailure::Corrupt)?
            .map(|value| descent.destination.entries.get_ref().slice_ref(value)))
    }

    // get_range is implemented by walking the tree. The returned Bytes
    // are all pinned to their containing leaf node's buffer, so for any long lived usage, they should be copied.
    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<'txn, T, S, A, C, R>, Self::Failure>
    {
        assert_valid_range(&range);
        let get_not_cleared_range =
            async |(start, end): (Bound<&Bytes>, Bound<&Bytes>),
                   entries: &mut SmallVec<[(Bytes, Bytes); 8]>| {
                let start_target = match start {
                    Included(key) => ConcatenatedBytes::from(&key[..]),
                    Excluded(key) => ConcatenatedBytes::new(key, &[0]),
                    Unbounded => ConcatenatedBytes::default(),
                };

                let reached_end = |key: &[u8]| match end {
                    Included(end) => key > &end[..],
                    Excluded(end) => key >= &end[..],
                    Unbounded => false,
                };

                let mut cursor = self.descend(&start_target).await?;
                let mut previous_start_bound: Bound<Bytes> = start.cloned();
                while entries.len() < limit {
                    let store_entry = match self.next_entry(&mut cursor).await? {
                        None => None,
                        Some((entry, leaf_bytes)) => {
                            let (store_key, store_value) =
                                entry.key_and_value().map_err(TreeFailure::Corrupt)?;
                            (!reached_end(store_key)).then(|| {
                                (
                                    leaf_bytes.slice_ref(store_key),
                                    leaf_bytes.slice_ref(store_value),
                                )
                            })
                        }
                    };
                    let store_entry = store_entry.map(|(key, value)| {
                        // check if there is a value buffered for the key that the store iterated to
                        let value = match self.buffered_delta.get(&key) {
                            None => value,
                            Some(Some(buffered)) => buffered.clone(),
                            Some(None) => {
                                panic!("get_not_cleared_range read a key inside a cleared range")
                            }
                        };
                        (key, value)
                    });
                    let buffered_end = match &store_entry {
                        Some((key, _)) => Excluded(key),
                        None => end,
                    };
                    let buffered_before_this = self
                        .buffered_delta
                        .overlapping((previous_start_bound.as_ref(), buffered_end))
                        .filter_map(|((start, _), write)| match (start, write) {
                            (Included(key), Some(value)) => Some((key.clone(), value.clone())),
                            _ => None,
                        });
                    entries.extend(
                        buffered_before_this
                            .chain(store_entry.clone())
                            .take(limit - entries.len()),
                    );
                    match store_entry {
                        Some((key, _)) => previous_start_bound = Excluded(key),
                        None => break,
                    }
                }

                Ok::<_, Self::Failure>(())
            };

        // only iterate in ranges that have not buffered a clear to avoid wasted store reads
        let gettable_ranges = self
            .buffered_delta
            .matching_ranges((range.start_bound(), range.end_bound()), |buffered_delta| {
                !matches!(buffered_delta, Some(None))
            });
        let mut entries = SmallVec::<[(Bytes, Bytes); 8]>::new();
        for (start, end) in gettable_ranges {
            if entries.len() >= limit {
                break;
            }
            get_not_cleared_range((start.as_ref(), end.as_ref()), &mut entries).await?;
        }

        Ok(entries.into_iter())
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

    fn set(
        &mut self,
        key: Bytes,
        mut value: impl Buf,
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

        self.buffered_delta
            .insert(&key..=&key, Some(value.copy_to_bytes(value.remaining())));
        Ok(())
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Self::Failure> {
        self.buffered_delta.insert(range, None);
        Ok(())
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
        self.flush().await.map_err(refused_write_as_failure)?;
        let mut from_header = self.get_volume_header().await?;
        from_header.epoch += 1;
        self.set_volume_header(from_header)
            .map_err(refused_write_as_failure)?;

        let new_volume = TreeVolumeId((self.shared.id_allocator)());
        self.inner
            .set(
                TreeKey::VolumeHeader(new_volume).into(),
                Bytes::from(&from_header),
            )
            .map_err(with_tree_failure)
            .map_err(refused_write_as_failure)?;

        Ok(new_volume)
    }
}
