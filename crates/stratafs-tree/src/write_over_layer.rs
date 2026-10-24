use bytes::BytesMut;
use smallvec::{SmallVec, smallvec};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};

use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeHeader};
use crate::invariants;
use crate::node::{NodeKind, ObservedNode};
use crate::slotted::SlottedList;
use crate::tree::TreeConfig;
use crate::volume::TreeTransaction;
use packing::PackingStrategy;

pub(crate) mod packing;
mod span_mutation_execution;
mod span_mutation_plan;
mod span_neighbors;
mod span_resolution;

pub(crate) struct LayerNode<R> {
    pub(crate) node: ObservedNode,
    pub(crate) steps: DescentSteps<R>,
}

// LayerSpan represents a set of nodes that should be treated as contiguous,
// that the sorted entries should be assigned into.
pub(crate) struct LayerSpan<R> {
    pub(crate) nodes: SmallVec<[LayerNode<R>; 2]>,
    pub(crate) entries: SlottedList<BytesMut, [u8], u32>,
}

impl<R> LayerSpan<R> {
    pub(crate) fn is_root(&self) -> bool {
        let is_root = self.nodes.first().is_none_or(|node| node.steps.is_empty());
        debug_assert!(
            !is_root || self.nodes.len() <= 1,
            "the root layer holds at most one node"
        );
        is_root
    }
}

pub(crate) struct Layer<R> {
    pub(crate) kind: NodeKind,
    pub(crate) spans: SmallVec<[LayerSpan<R>; 1]>,
}

// LayerNodeMutation is one planned change to a node in a layer
enum LayerNodeMutation<R> {
    Create {
        separator: Box<[u8]>,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Update {
        node: LayerNode<R>,
        separator: Box<[u8]>,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Delete {
        node: LayerNode<R>,
    },
    // if an internal layer is left with a single entry, it gets "promoted" into the parent layer
    Promote {
        separator: Box<[u8]>,
        child: NodeId,
    },
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
        packing: PackingStrategy,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        debug_assert!(invariants::layer_is_well_formed(&layer));
        let mut layer = layer;
        while !layer.spans.is_empty() {
            layer = self.fit_layer(volume, layer, packing).await?;
        }
        Ok(())
    }

    // fit_layer handles actually fitting the given entries to the given nodes.
    // First, any span too small to fill a node pulls in its lighter neighbor, and spans that end up
    // sharing a node are joined. Then each span's entries are packed into nodes that each meet the
    // minimum, reusing the span's nodes, creating new ones if they overflow, and deleting the ones
    // left over. An internal span left with a single entry is the only node on its layer,
    // so it is deleted and its child is promoted into the parent layer.
    // When writing nodes into the tree, it makes sure to write over a new node if the node's epoch
    // is older than the volume's, otherwise it mutates in place. It returns the desired state of
    // the parent layer, which at the root is the volume header.
    async fn fit_layer(
        &mut self,
        volume: VolumeHeader,
        layer: Layer<C::Range>,
        packing: PackingStrategy,
    ) -> Result<
        Layer<C::Range>,
        StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>,
    > {
        let independent_spans = self
            .resolve_independent_spans(layer.kind, layer.spans)
            .await
            .map_err(StoreError::Failed)?;

        let plans = independent_spans
            .into_iter()
            .map(|span| self.create_span_mutation_plan(layer.kind, span, packing))
            .collect::<Result<SmallVec<[_; 1]>, _>>()
            .map_err(StoreError::Failed)?;
        let spans = self
            .execute_span_mutation_plans(volume, layer.kind, plans)
            .await?;
        Ok(Layer {
            kind: NodeKind::Internal,
            spans,
        })
    }
}
