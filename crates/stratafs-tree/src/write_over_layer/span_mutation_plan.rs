use std::ops::Range;

use bytes::BytesMut;
use smallvec::{SmallVec, smallvec};
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};
use zerocopy::{FromBytes, KnownLayout};

use super::packing::PackingStrategy;
use super::span_resolution::{IndependentResolvedSpan, node_lower_bound};
use super::{LayerNode, LayerNodeMutation};
use crate::error::TreeFailure;
use crate::header::NodeId;
use crate::node::{InternalEntry, LEFTMOST_SEPARATOR, LeafEntry, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::TreeTransaction;

pub(super) struct SpanMutationPlan<R> {
    pub(super) mutations: SmallVec<[LayerNodeMutation<R>; 3]>,
}

impl<R> SpanMutationPlan<R> {
    pub(super) fn first_existing_node(&self) -> &LayerNode<R> {
        self.mutations
            .iter()
            .find_map(existing_node)
            .expect("a plan always changes at least one existing node")
    }

    pub(super) fn last_existing_node(&self) -> &LayerNode<R> {
        self.mutations
            .iter()
            .rev()
            .find_map(existing_node)
            .expect("a plan always changes at least one existing node")
    }
}

fn existing_node<R>(mutation: &LayerNodeMutation<R>) -> Option<&LayerNode<R>> {
    match mutation {
        LayerNodeMutation::Update { node, .. } | LayerNodeMutation::Delete { node } => Some(node),
        LayerNodeMutation::Create { .. } => None,
    }
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) fn create_span_mutation_plan(
        &self,
        kind: NodeKind,
        span: IndependentResolvedSpan<C::Range>,
        packing: PackingStrategy,
    ) -> Result<SpanMutationPlan<C::Range>, TreeFailure<T::Failure, S::Rejection>> {
        let span = span.into_span();
        let is_root = span.is_root();
        if span.entries.len() == 0 {
            if is_root {
                let root = span
                    .nodes
                    .into_iter()
                    .next()
                    .expect("a span always has a node");
                return Ok(SpanMutationPlan {
                    mutations: smallvec![LayerNodeMutation::Update {
                        node: root,
                        separator: Box::from(LEFTMOST_SEPARATOR),
                        entries: span.entries,
                    }],
                });
            }
            return Ok(SpanMutationPlan {
                mutations: span
                    .nodes
                    .into_iter()
                    .map(|node| LayerNodeMutation::Delete { node })
                    .collect(),
            });
        }

        // PHASE 1: Determine how many nodes we need to evenly distribute the entries without under/overflowing
        let min_node_size = if is_root { 0 } else { C::MERGE_BELOW_SIZE };
        let optimal_boundaries_plan = packing.boundaries(
            span.entries.len(),
            |range| Self::node_size(kind, &span.entries, range),
            min_node_size,
            C::NODE_CAPACITY,
        );

        // PHASE 2: create a plan about what we are going to do to the nodes in this layer,
        // according to the optimal boundaries we just computed
        let mut span_lower_bound = Some(Box::from(
            node_lower_bound(&span.nodes.first().steps).map_err(TreeFailure::Corrupt)?,
        ));
        let mut entries = span.entries;
        let mut chunks: SmallVec<[_; 3]> = optimal_boundaries_plan
            [1..optimal_boundaries_plan.len() - 1]
            .iter()
            .rev()
            .map(|&boundary| entries.split_off(boundary))
            .collect();
        chunks.push(entries);
        let mut mutations: SmallVec<[LayerNodeMutation<C::Range>; 3]> =
            SmallVec::with_capacity(chunks.len());
        let mut nodes = span.nodes.into_iter();
        for mut chunk in chunks.into_iter().rev() {
            let first_key = separator_for_parent(kind, &mut chunk).map_err(TreeFailure::Corrupt)?;
            let separator = span_lower_bound.take().unwrap_or(first_key);
            mutations.push(match nodes.next() {
                Some(node) => LayerNodeMutation::Update {
                    node,
                    separator,
                    entries: chunk,
                },
                None => LayerNodeMutation::Create {
                    separator,
                    entries: chunk,
                },
            });
        }
        // any remaining nodes dont have any entries, and should therefore be deleted
        mutations.extend(nodes.map(|node| LayerNodeMutation::Delete { node }));

        Ok(SpanMutationPlan { mutations })
    }

    pub(super) fn node_size(
        kind: NodeKind,
        entries: &SlottedList<BytesMut, [u8], C::Range>,
        range: Range<usize>,
    ) -> usize {
        C::EMPTY_NODE_SIZE + entries.size_before(range.end)
            - entries.size_before(range.start)
            - match kind {
                NodeKind::Leaf => 0,
                NodeKind::Internal => entries
                    .get(range.start)
                    .and_then(Result::ok)
                    .map_or(0, |entry| entry.len().saturating_sub(size_of::<NodeId>())),
            }
    }
}

fn separator_for_parent<R: AddressableRange>(
    kind: NodeKind,
    right_half: &mut SlottedList<BytesMut, [u8], R>,
) -> Result<Box<[u8]>, CorruptValue> {
    let first_entry = right_half
        .get(0)
        .expect("a split leaves both halves with entries")?;
    match kind {
        NodeKind::Leaf => {
            let (key, _) = LeafEntry::<R>::ref_from_bytes(first_entry)
                .map_err(|_| CorruptValue::WrongLength)?
                .key_and_value()?;
            Ok(Box::from(key))
        }
        NodeKind::Internal => {
            let first_entry = InternalEntry::ref_from_bytes(first_entry)
                .map_err(|_| CorruptValue::WrongLength)?;
            let (child, separator) = (first_entry.child, Box::from(&first_entry.key));
            let leftmost_length = InternalEntry::size_for_metadata(LEFTMOST_SEPARATOR.len())
                .expect("an internal entry fits in memory");
            InternalEntry::mut_from_bytes(right_half.replace_zeroed(0, leftmost_length))
                .expect("the replaced space is sized for a leftmost entry")
                .child = child;
            Ok(separator)
        }
    }
}
