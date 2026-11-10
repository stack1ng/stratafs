use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_channel::oneshot;
use stratafs_runtime::Runtime;
use stratafs_serialized::serialized_value;
use stratafs_store::content::ContentReference;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
};
use stratafs_store::store::Store;
use zerocopy::little_endian::{U64, U128};

use crate::branch_observation::SharedWatches;
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
    const GARBAGE_COLLECTION_INTERVAL: Duration = Duration::from_secs(60);
    // GARBAGE_COLLECTION_BATCH is the most jobs the garbage collector applies in one transaction.
    const GARBAGE_COLLECTION_BATCH: usize = 32;

    // referenced_content is a leaf entry's reference to a blob, if it has one. The tree counts those references,
    // so the garbage collector deletes a blob once no leaf entry references it.
    fn referenced_content(_key: &[u8], _value: &[u8]) -> Option<ContentReference> {
        None
    }
}

// MetadataRejection is the rejection of a store's metadata store.
pub type MetadataRejection<S> = <<S as Store>::Metadata as KeyValueStore>::Rejection;

// MetadataFailure is the failure of a store's metadata store.
pub type MetadataFailure<S> = <<S as Store>::Metadata as KeyValueStore>::Failure;

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

pub struct Tree<S: Store, A, C>(Arc<TreeShared<S, A, C>>);

pub(crate) struct TreeShared<S: Store, A, C> {
    pub(crate) store: S,
    pub(crate) id_allocator: A,
    // watches holds each watch that waiters share, for as long as one of them still waits on it
    pub(crate) watches: Mutex<SharedWatches<MetadataFailure<S>>>,
    // runtime runs each opened volume's garbage collector
    pub(crate) runtime: Arc<dyn Runtime>,
    config: PhantomData<C>,
}

impl<S, A, C> Tree<S, A, C>
where
    S: Store,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // new creates a new tree in the store. It validates that either there is no existing config,
    // or that the existing config matches the configured one.
    pub async fn new(
        store: S,
        id_allocator: A,
        runtime: Arc<dyn Runtime>,
    ) -> Result<
        Self,
        TransactError<
            MetadataRejection<S>,
            MetadataFailure<S>,
            PinnedConfigError<MetadataRejection<S>, MetadataFailure<S>>,
        >,
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
            .metadata()
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
            watches: Mutex::default(),
            runtime,
            config: PhantomData,
        })))
    }
}

impl<S, A, C> ForkableKeyValueStore for Tree<S, A, C>
where
    S: Store,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    type Failure = TreeFailure<MetadataFailure<S>, MetadataRejection<S>>;
    type CreateError = TransactError<
        MetadataRejection<S>,
        MetadataFailure<S>,
        StoreError<SetRejection<MetadataRejection<S>>, MetadataFailure<S>>,
    >;
    type VolumeId = TreeVolumeId;
    type Volume = TreeVolume<S, A, C>;

    // create creates a new volume with no contents.
    async fn create(&self) -> Result<TreeVolumeId, Self::CreateError> {
        let id = TreeVolumeId((self.0.id_allocator)());
        let root = NodeId(U128::new((self.0.id_allocator)()));
        self.0
            .store
            .metadata()
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

    // open opens an existing volume by its id, starting its garbage collector, which runs until the volume is dropped.
    // If the volume does not exist, it returns UnknownVolume.
    async fn open(
        &self,
        id: TreeVolumeId,
    ) -> Result<Self::Volume, StoreError<UnknownVolume, Self::Failure>> {
        let header = self
            .0
            .store
            .metadata()
            .read_transact(move |transaction| {
                Box::pin(async move { transaction.get(TreeKey::VolumeHeader(id).into()).await })
            })
            .await;
        match header {
            Ok(Some(_)) => {
                let (stop_garbage_collection, stopped) = oneshot::channel();
                let garbage_collection = TreeShared::collect_garbage_until(
                    Arc::downgrade(&self.0),
                    Arc::clone(&self.0.runtime),
                    id,
                    stopped,
                );
                self.0.runtime.spawn(Box::pin(garbage_collection));
                Ok(TreeVolume {
                    shared: Arc::clone(&self.0),
                    id,
                    _stop_garbage_collection: stop_garbage_collection,
                })
            }
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
