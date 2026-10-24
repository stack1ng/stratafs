use std::ops::Range;

use bytes::BytesMut;
use futures_util::future::join;
use smallvec::{SmallVec, smallvec};
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};
use zerocopy::{FromBytes, KnownLayout};

use super::packing::PackingStrategy;
use super::span_neighbors::{Neighbor, Side};
use super::{LayerNodeMutation, LayerSpan};
use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::NodeId;
use crate::invariants;
use crate::node::{InternalEntry, LEFTMOST_SEPARATOR, LeafEntry, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::TreeTransaction;

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) async fn create_span_mutation_plan(
        &self,
        kind: NodeKind,
        mut span: LayerSpan<C::Range>,
        packing: PackingStrategy,
    ) -> Result<SmallVec<[LayerNodeMutation<C::Range>; 3]>, TreeFailure<T::Failure, S::Rejection>>
    {
        debug_assert!(invariants::span_is_well_formed(kind, &span));
        let is_root = span.is_root();
        if span.entries.len() == 0 {
            if is_root {
                return Ok(smallvec![LayerNodeMutation::Update {
                    node: span.nodes.first().node,
                    separator: Box::from(LEFTMOST_SEPARATOR),
                    entries: span.entries,
                }]);
            }
            let mut delete_all_plan: SmallVec<[LayerNodeMutation<C::Range>; 3]> = SmallVec::new();
            for leftover in span.nodes.iter() {
                delete_all_plan.push(LayerNodeMutation::Delete {
                    node: leftover.node,
                });
            }
            return Ok(delete_all_plan);
        }

        // PHASE 1: Determine how many nodes we need to evenly distribute the entries without under/overflowing
        let min_node_size = if is_root { 0 } else { C::MERGE_BELOW_SIZE };
        let optimal_boundaries_plan = loop {
            let node_size = |range: Range<usize>| {
                C::EMPTY_NODE_SIZE + span.entries.size_before(range.end)
                    - span.entries.size_before(range.start)
                    - match kind {
                        NodeKind::Leaf => 0,
                        NodeKind::Internal => span
                            .entries
                            .get(range.start)
                            .and_then(Result::ok)
                            .map_or(0, |entry| entry.len().saturating_sub(size_of::<NodeId>())),
                    }
            };
            let boundaries = packing.boundaries(
                span.entries.len(),
                node_size,
                min_node_size,
                C::NODE_CAPACITY,
            );

            // check to see if we need to merge with a neighbor to the span
            let packed_into_one_underfull_node =
                boundaries.len() == 2 && node_size(0..span.entries.len()) < min_node_size;
            if !packed_into_one_underfull_node {
                break boundaries;
            }

            // check both neighbors as candidates to merge with.
            // Only fetch cousins if we have no immediate neighbors,
            // since it is much slower
            let candidates = {
                let neighbors = self
                    .find_span_neighbors(&span)
                    .expect("only the root has no neighbors");
                match (neighbors.left, neighbors.right) {
                    // both neighbors are immediate, meaning they have the same read cost, so do them in parallel
                    (Neighbor::Immediate(left), Neighbor::Immediate(right)) => {
                        let (left, right) = join(left, right).await;
                        [Some(left?), Some(right?)]
                    }

                    // only one neighbor is immediate, only read it since its cheaper than reading the extended neighbor
                    (Neighbor::Immediate(left), Neighbor::MaybeExtended(_)) => {
                        [Some(left.await?), None]
                    }
                    (Neighbor::MaybeExtended(_), Neighbor::Immediate(right)) => {
                        [None, Some(right.await?)]
                    }

                    // both neighbors are extended, for the same reason as the first case, read both in parallel
                    (Neighbor::MaybeExtended(left), Neighbor::MaybeExtended(right)) => {
                        let (left, right) = join(left, right).await;
                        [left?, right?]
                    }
                }
            };
            let [left, right] = candidates;
            let (merge_side, (node, entries)) = match (left, right) {
                (None, None) => {
                    // being here means the span covers its layer's entire domain, so it fits in one node,
                    // and the layers above it collapse during the ascent until that node is the root,
                    // where being underfull is accepted.
                    break boundaries;
                }
                (Some(left), Some(right)) if right.1.stored_length() < left.1.stored_length() => {
                    (Side::Right, right)
                }
                (Some(left), _) => (Side::Left, left),
                (None, Some(right)) => (Side::Right, right),
            };
            let mut neighbor_entries = resolve_node_relative_first_separator(
                kind,
                SlottedList::from(entries),
                &node.steps,
            )
            .map_err(TreeFailure::Corrupt)?;
            match merge_side {
                Side::Left => {
                    neighbor_entries.extend_from_list(&span.entries);
                    span.entries = neighbor_entries;
                    span.nodes.insert(0, node);
                }
                Side::Right => {
                    span.entries.extend_from_list(&neighbor_entries);
                    span.nodes.push(node);
                }
            }
        };

        // PHASE 2: create a plan about what we are going to do to the nodes in this layer,
        // according to the optimal boundaries we just computed
        let mut entries = span.entries;
        let mut chunks: SmallVec<[_; 3]> = optimal_boundaries_plan
            [1..optimal_boundaries_plan.len() - 1]
            .iter()
            .rev()
            .map(|&boundary| entries.split_off(boundary))
            .collect();
        chunks.push(entries);
        let chunk_count = chunks.len();
        let mut plan: SmallVec<[LayerNodeMutation<C::Range>; 3]> =
            SmallVec::with_capacity(chunk_count);
        for (i, mut chunk) in chunks.into_iter().rev().enumerate() {
            let separator = separator_for_parent(kind, &mut chunk).map_err(TreeFailure::Corrupt)?;
            match span.nodes.get(i) {
                Some(l) => plan.push(LayerNodeMutation::Update {
                    node: l.node,
                    separator,
                    entries: chunk,
                }),
                None => plan.push(LayerNodeMutation::Create {
                    separator,
                    entries: chunk,
                }),
            }
        }
        // any remaining nodes dont have any entries, and should therefore be deleted
        for leftover in span.nodes.iter().skip(chunk_count) {
            plan.push(LayerNodeMutation::Delete {
                node: leftover.node,
            });
        }

        Ok(plan)
    }
}

// resolve_node_relative_first_separator is a utility for turning an entry list directly taken from a node,
// into a globally sortable subset of the layer's entry domain.
// Internal node entry lists always have the 0th separator set to "" as a catchall, so we simply correct for this
// by reading the node's lower bound (through ancestry, NOT with more store reads) and using that as the inferred proper narrow separator
fn resolve_node_relative_first_separator<R: AddressableRange>(
    kind: NodeKind,
    node_relative_entries: SlottedList<BytesMut, [u8], R>,
    node_ancestry: &DescentSteps<R>,
) -> Result<SlottedList<BytesMut, [u8], R>, CorruptValue> {
    match kind {
        NodeKind::Leaf => Ok(node_relative_entries),
        NodeKind::Internal => {
            let separator = match node_ancestry.iter().rev().find(|step| step.position > 0) {
                Some(step) => {
                    &step
                        .entries
                        .get(step.position)
                        .expect("a step's position is within its parent")?
                        .key
                }
                None => LEFTMOST_SEPARATOR,
            };
            let mut entries = node_relative_entries.into_typed::<InternalEntry>();
            let child = entries
                .get(0)
                .expect("an internal node being merged has a child")?
                .child;
            entries.replace_entry(0, child, separator);
            Ok(entries.into_untyped())
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
