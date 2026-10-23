use bytes::BytesMut;
use smallvec::SmallVec;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};

use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::VolumeHeader;
use crate::node::{NodeHeader, NodeKind, ObservedNode};
use crate::slotted::SlottedList;
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::TreeTransaction;

pub(crate) struct LayerNode<R> {
    pub(crate) node: ObservedNode,
    pub(crate) steps: DescentSteps<R>,
}

pub(crate) struct LayerSpan<R> {
    pub(crate) nodes: SmallVec<[LayerNode<R>; 2]>,
    pub(crate) entries: SlottedList<BytesMut, [u8], R>,
}

pub(crate) struct Layer<R> {
    pub(crate) kind: NodeKind,
    pub(crate) spans: SmallVec<[LayerSpan<R>; 1]>,
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(crate) async fn write_over_layer(
        &mut self,
        volume: VolumeHeader,
        layer: Layer<C::Range>,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        todo!()
    }

    // fit_layer handles actually fitting the given entries to the given nodes.
    // It packs entries left to right, dropping nodes that dont end up being used,
    // and creating new ones if we overflow the given set of nodes. Then, if the tail (rightmost)
    // node ends up being underfull, it merges with a neighbor. When writing nodes into the tree,
    // it makes sure to write over a new node if the node's epoch is older than the volume's,
    // otherwise it mutates in place. It returns the desired state of the parent layer.
    async fn fit_layer(&mut self, layer: Layer<C::Range>) {
        for span in &layer.spans {
            // PHASE 1: Determine how many nodes we need to evenly distribute the entries without under/overflowing
            let entries_size =
                size_of::<NodeHeader>() + span.entries.stored_length() - C::EMPTY_NODE_SIZE;
            let estimated_needed_node_count =
                entries_size.div_ceil(C::NODE_CAPACITY - C::EMPTY_NODE_SIZE);
        }
    }
}
