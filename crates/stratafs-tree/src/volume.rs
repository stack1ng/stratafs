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
use zerocopy::IntoBytes;

use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeEpoch};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
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

const MAX_DEPTH: usize = 64;

struct DescentStep<R, T: ?Sized, P> {
    entries: SlottedList<Bytes, T, R>,
    epoch: VolumeEpoch,
    position: P,
}

struct DescentPath<R> {
    steps: Vec<DescentStep<R, InternalEntry, usize>>,
    destination: DescentStep<
        R,
        LeafEntry,
        // leaf entries might not have a match, so this expects the binary search result
        Result<usize, usize>,
    >,
}

#[derive(Debug, thiserror::Error)]
pub enum DescentError<F> {
    #[error(transparent)]
    Get(#[from] F),
    #[error(transparent)]
    Corrupt(CorruptValue),
}

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: ReadKeyValueTransaction,
    R: AddressableRange,
{
    async fn descend(
        &self,
        root: NodeId,
        key: &[u8],
    ) -> Result<DescentPath<R>, DescentError<T::Failure>> {
        let mut steps: Vec<DescentStep<R, InternalEntry, usize>> = Vec::new();
        let mut current = root;
        for _ in 0..MAX_DEPTH {
            let node_bytes = self
                .inner
                .get(TreeKey::Node(current).into())
                .await?
                .ok_or(DescentError::Corrupt(CorruptValue::MissingNode))?;
            let (epoch, node) =
                Node::<Bytes, R>::read(node_bytes).map_err(DescentError::Corrupt)?;

            match node {
                Node::Leaf(entries) => {
                    let position = entries
                        .binary_search_by(|leaf| {
                            let (leaf_key, _) = leaf.key_and_value()?;
                            Ok(leaf_key.cmp(key))
                        })
                        .map_err(DescentError::Corrupt)?;
                    return Ok(DescentPath {
                        steps,
                        destination: DescentStep {
                            entries,
                            epoch,
                            position,
                        },
                    });
                }
                Node::Internal(entries) => {
                    let applicable_separator_index = match entries
                        .binary_search_by(|separator| Ok(separator.key.cmp(key)))
                        .map_err(DescentError::Corrupt)?
                    {
                        Ok(index) => index,
                        Err(0) => {
                            return Err(DescentError::Corrupt(
                                CorruptValue::KeyBeforeFirstSeparator,
                            ));
                        }
                        // if there is not an exact match, we are in the applicable range of the separator that is to the left of this index.
                        Err(index) => index - 1,
                    };

                    current = entries
                        .get(applicable_separator_index)
                        .expect("the index is in the range")
                        .map_err(DescentError::Corrupt)?
                        .child;

                    steps.push(DescentStep {
                        entries,
                        epoch,
                        position: applicable_separator_index,
                    });
                }
            }
        }
        Err(DescentError::Corrupt(CorruptValue::TreeTooDeep))
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
