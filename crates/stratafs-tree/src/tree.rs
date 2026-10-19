use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume, VolumeId};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
};
use zerocopy::little_endian::{U64, U128};

use crate::error::TreeFailure;
use crate::header::{NodeId, TreeVolumeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{Node, NodeHeader, NodeKind};
use crate::serialized::{CorruptValue, serialized_value};
use crate::slotted::{AddressableRange, SlottedList};
use crate::volume::TreeVolume;

#[derive(Debug, Clone, Copy)]
pub struct TreeConfig<R, const NODE_CAPACITY: usize> {
    pub(crate) merge_below_percent: u8,
    address: PhantomData<R>,
}

impl<R: AddressableRange, const NODE_CAPACITY: usize> TreeConfig<R, NODE_CAPACITY> {
    pub(crate) const LARGEST_ENTRY_SIZE: usize =
        (NODE_CAPACITY - size_of::<NodeHeader>() - size_of::<R::Stored>()) / 3
            - size_of::<R::Stored>();

    pub fn new(merge_below_percent: u8) -> Self {
        const {
            assert!(
                NODE_CAPACITY <= R::MAX,
                "the node capacity exceeds the addressable range"
            );
            assert!(
                NODE_CAPACITY >= R::SMALLEST_OPTIMAL_CAPACITY,
                "the node capacity is below the smallest optimal capacity for this addressable range"
            );
            assert!(
                Self::LARGEST_ENTRY_SIZE >= size_of::<NodeId>(),
                "the node capacity is too small to hold a separator entry"
            );
        };
        Self {
            merge_below_percent,
            address: PhantomData,
        }
    }
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

pub struct Tree<S, A, R, const NODE_CAPACITY: usize>(Arc<TreeShared<S, A, R, NODE_CAPACITY>>);

pub(crate) struct TreeShared<S, A, R, const NODE_CAPACITY: usize> {
    pub(crate) store: S,
    pub(crate) config: TreeConfig<R, NODE_CAPACITY>,
    pub(crate) id_allocator: A,
}

impl<S, A, R, const NODE_CAPACITY: usize> Tree<S, A, R, NODE_CAPACITY>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    // new creates a new tree in the store. It validates that either there is no existing config,
    // or that the existing config matches the configured one.
    pub async fn new(
        store: S,
        config: TreeConfig<R, NODE_CAPACITY>,
        id_allocator: A,
    ) -> Result<
        Self,
        TransactError<S::Rejection, S::Failure, PinnedConfigError<S::Rejection, S::Failure>>,
    > {
        let configured = PinnedConfig {
            node_capacity: U64::new(NODE_CAPACITY as u64),
            address_length: size_of::<R::Stored>() as u8,
            merge_below_percent: config.merge_below_percent,
        };
        store
            .transact(move |mut transaction| {
                Box::pin(async move {
                    let key = Bytes::from(TreeKey::Config);
                    let stored = transaction
                        .get(key.clone())
                        .await
                        .map_err(|failure| PinnedConfigError::Store(StoreError::Failed(failure)))?;
                    match stored {
                        None => transaction
                            .set(key, Bytes::from(&configured))
                            .await
                            .map_err(PinnedConfigError::Store),
                        Some(stored) => match PinnedConfig::view(&stored) {
                            Ok(stored) if *stored == configured => Ok(()),
                            Ok(stored) => Err(PinnedConfigError::Mismatch {
                                stored: *stored,
                                configured,
                            }),
                            Err(corrupt) => Err(PinnedConfigError::Corrupt(corrupt)),
                        },
                    }
                })
            })
            .await?;
        Ok(Self(Arc::new(TreeShared {
            store,
            config,
            id_allocator,
        })))
    }
}

impl<S, A, R, const NODE_CAPACITY: usize> ForkableKeyValueStore for Tree<S, A, R, NODE_CAPACITY>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    type Failure = TreeFailure<S::Failure, S::Rejection>;
    type CreateError =
        TransactError<S::Rejection, S::Failure, StoreError<SetRejection<S::Rejection>, S::Failure>>;
    type Volume = TreeVolume<S, A, R, NODE_CAPACITY>;

    // create creates a new volume with no contents.
    async fn create(&self) -> Result<VolumeId, Self::CreateError> {
        let id = TreeVolumeId((self.0.id_allocator)());
        let root = NodeId(U128::new((self.0.id_allocator)()));
        self.0
            .store
            .transact(move |mut transaction| {
                Box::pin(async move {
                    transaction
                        .set(
                            TreeKey::VolumeHeader(id).into(),
                            Bytes::from(&VolumeHeader {
                                root,
                                epoch: VolumeEpoch(U64::new(0)),
                            }),
                        )
                        .await?;
                    transaction
                        .set(
                            TreeKey::Node(root).into(),
                            Node::write(
                                NodeHeader {
                                    kind: NodeKind::Leaf,
                                    epoch: VolumeEpoch(U64::new(0)),
                                },
                                SlottedList::<
                                    BytesMut,
                                    // we aren't storing any values yet, so we intentionally make it so we cant store any with the infallible type.
                                    Infallible,
                                    R,
                                >::empty(),
                            ),
                        )
                        .await
                })
            })
            .await?;
        Ok(VolumeId(Bytes::copy_from_slice(&id.0.to_be_bytes())))
    }

    // open opens an existing volume by its id. If the volume does not exist, it returns UnknownVolume.
    async fn open(
        &self,
        id: VolumeId,
    ) -> Result<Self::Volume, StoreError<UnknownVolume, Self::Failure>> {
        todo!()
    }
}

#[cfg(test)]
mod tests;
