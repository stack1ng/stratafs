use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use stratafs_serialized::serialized_value;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
};
use zerocopy::little_endian::{U64, U128};

use crate::error::TreeFailure;
use crate::header::{NodeId, TreeVolumeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{Node, NodeHeader, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::volume::TreeVolume;

pub trait TreeConfig: Send + Sync + 'static {
    type Range: AddressableRange;
    const NODE_CAPACITY: usize;
    const MERGE_BELOW_PERCENT: u8;
}

pub(crate) trait TreeConfigLimits: TreeConfig {
    const LARGEST_ENTRY_SIZE: usize;
    const MERGE_BELOW_SIZE: usize;
    const EMPTY_NODE_SIZE: usize;
}

impl<C: TreeConfig> TreeConfigLimits for C {
    const LARGEST_ENTRY_SIZE: usize = (C::NODE_CAPACITY
        - size_of::<NodeHeader>()
        - size_of::<<C::Range as AddressableRange>::Stored>())
        / 3
        - size_of::<<C::Range as AddressableRange>::Stored>();
    const MERGE_BELOW_SIZE: usize =
        (C::NODE_CAPACITY as u64 * C::MERGE_BELOW_PERCENT as u64 / 100) as usize;
    const EMPTY_NODE_SIZE: usize =
        size_of::<NodeHeader>() + size_of::<<C::Range as AddressableRange>::Stored>();
}

serialized_value! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PinnedConfig {
        pub node_capacity: U64,
        pub address_length: u8,
        pub merge_below_percent: u8,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PinnedConfigError<R, F> {
    #[error(
        "the store's tree was created with {stored:?}, but this tree is configured with {configured:?}"
    )]
    Mismatch {
        stored: PinnedConfig,
        configured: PinnedConfig,
    },
    #[error(transparent)]
    Corrupt(CorruptValue),
    #[error(transparent)]
    Store(StoreError<SetRejection<R>, F>),
}

pub struct Tree<S, A, C>(Arc<TreeShared<S, A, C>>);

pub(crate) struct TreeShared<S, A, C> {
    pub(crate) store: S,
    pub(crate) id_allocator: A,
    config: PhantomData<C>,
}

impl<S, A, C> Tree<S, A, C>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // new creates a new tree in the store. It validates that either there is no existing config,
    // or that the existing config matches the configured one.
    pub async fn new(
        store: S,
        id_allocator: A,
    ) -> Result<
        Self,
        TransactError<S::Rejection, S::Failure, PinnedConfigError<S::Rejection, S::Failure>>,
    > {
        const {
            assert!(
                C::NODE_CAPACITY <= <C::Range as AddressableRange>::MAX,
                "the node capacity exceeds the addressable range"
            );
            assert!(
                C::NODE_CAPACITY >= <C::Range as AddressableRange>::SMALLEST_OPTIMAL_CAPACITY,
                "the node capacity is below the smallest optimal capacity for this addressable range"
            );
            assert!(
                C::LARGEST_ENTRY_SIZE >= size_of::<NodeId>(),
                "the node capacity is too small to hold a separator entry"
            );
            assert!(
                C::MERGE_BELOW_SIZE
                    > C::EMPTY_NODE_SIZE
                        + size_of::<<C::Range as AddressableRange>::Stored>()
                        + size_of::<NodeId>(),
                "the merge threshold must be above the size of an internal node with one child, or a node can be left without a sibling to merge with"
            );
            assert!(
                C::MERGE_BELOW_SIZE
                    <= C::EMPTY_NODE_SIZE + (C::NODE_CAPACITY - C::EMPTY_NODE_SIZE) / 3,
                "the merge threshold must be at or below the smallest half a split can produce"
            );
        };
        let configured = PinnedConfig {
            node_capacity: U64::new(C::NODE_CAPACITY as u64),
            address_length: size_of::<<C::Range as AddressableRange>::Stored>() as u8,
            merge_below_percent: C::MERGE_BELOW_PERCENT,
        };
        store
            .transact(move |mut transaction| {
                Box::pin(async move {
                    let key = Bytes::from(TreeKey::Config);
                    let stored = transaction
                        .get(key.clone())
                        .await
                        .map_err(|failure| PinnedConfigError::Store(StoreError::Failed(failure)))?;
                    let checked = match stored {
                        None => transaction
                            .set(key, Bytes::from(&configured))
                            .map_err(PinnedConfigError::Store),
                        Some(stored) => match PinnedConfig::view(&stored) {
                            Ok(stored) if *stored == configured => Ok(()),
                            Ok(stored) => Err(PinnedConfigError::Mismatch {
                                stored: *stored,
                                configured,
                            }),
                            Err(corrupt) => Err(PinnedConfigError::Corrupt(corrupt.into())),
                        },
                    };
                    checked.map(|()| ((), transaction))
                })
            })
            .await?;
        Ok(Self(Arc::new(TreeShared {
            store,
            id_allocator,
            config: PhantomData,
        })))
    }
}

impl<S, A, C> ForkableKeyValueStore for Tree<S, A, C>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    type Failure = TreeFailure<S::Failure, S::Rejection>;
    type CreateError =
        TransactError<S::Rejection, S::Failure, StoreError<SetRejection<S::Rejection>, S::Failure>>;
    type VolumeId = TreeVolumeId;
    type Volume = TreeVolume<S, A, C>;

    // create creates a new volume with no contents.
    async fn create(&self) -> Result<TreeVolumeId, Self::CreateError> {
        let id = TreeVolumeId((self.0.id_allocator)());
        let root = NodeId(U128::new((self.0.id_allocator)()));
        self.0
            .store
            .transact(move |mut transaction| {
                Box::pin(async move {
                    transaction.set(
                        TreeKey::VolumeHeader(id).into(),
                        Bytes::from(&VolumeHeader {
                            root,
                            epoch: VolumeEpoch(U64::new(0)),
                        }),
                    )?;
                    transaction
                        .set(
                            TreeKey::Node(root).into(),
                            Node::write(
                                &NodeHeader {
                                    kind: NodeKind::Leaf,
                                    epoch: VolumeEpoch(U64::new(0)),
                                },
                                &SlottedList::<
                                    BytesMut,
                                    // we aren't storing any values yet, so we intentionally make it so we cant store any with the infallible type.
                                    Infallible,
                                    C::Range,
                                >::empty(),
                            ),
                        )
                        .map(|()| ((), transaction))
                })
            })
            .await?;
        Ok(id)
    }

    // open opens an existing volume by its id. If the volume does not exist, it returns UnknownVolume.
    async fn open(
        &self,
        id: TreeVolumeId,
    ) -> Result<Self::Volume, StoreError<UnknownVolume, Self::Failure>> {
        let header = self
            .0
            .store
            .read_transact(move |transaction| {
                Box::pin(async move { transaction.get(TreeKey::VolumeHeader(id).into()).await })
            })
            .await;
        match header {
            Ok(Some(_)) => Ok(TreeVolume {
                shared: Arc::clone(&self.0),
                id,
            }),
            Ok(None) => Err(StoreError::Rejected(UnknownVolume)),
            Err(TransactError::Rejected(rejection)) => Err(StoreError::Failed(
                TreeFailure::RefusedTransaction(rejection),
            )),
            Err(TransactError::Failed(failure) | TransactError::Aborted(failure)) => {
                Err(StoreError::Failed(TreeFailure::Store(failure)))
            }
        }
    }
}

#[cfg(test)]
mod tests;
