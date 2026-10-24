use std::ops::Range;

use bytes::BytesMut;
use futures_util::future::{join, try_join_all};
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

// IndependentResolvedSpan represents a span that does not overlap with any other spans in its Layer,
// and has been resolved with the resolve_span helper.
pub(super) struct IndependentResolvedSpan<R>(LayerSpan<R>);

struct PulledSpan<R> {
    span: LayerSpan<R>,
    left_pull_entry_counts: SmallVec<[usize; 1]>,
    right_pull_entry_counts: SmallVec<[usize; 1]>,
}

impl<R> PulledSpan<R> {
    fn without_pulls(span: LayerSpan<R>) -> Self {
        PulledSpan {
            span,
            left_pull_entry_counts: SmallVec::new(),
            right_pull_entry_counts: SmallVec::new(),
        }
    }
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) async fn resolve_independent_spans(
        &self,
        kind: NodeKind,
        spans: SmallVec<[LayerSpan<C::Range>; 1]>,
    ) -> Result<
        SmallVec<[IndependentResolvedSpan<C::Range>; 1]>,
        TreeFailure<T::Failure, S::Rejection>,
    > {
        debug_assert!(invariants::spans_are_ordered(&spans));
        let mut unresolved = spans;
        loop {
            let resolved = try_join_all(
                unresolved
                    .into_iter()
                    .map(|span| self.resolve_span(kind, span)),
            )
            .await?;
            if !resolved
                .windows(2)
                .any(|pair| spans_share_a_node(&pair[0].span, &pair[1].span))
            {
                return Ok(resolved
                    .into_iter()
                    .map(|pulled| {
                        debug_assert!(invariants::span_is_well_formed(kind, &pulled.span));
                        IndependentResolvedSpan(pulled.span)
                    })
                    .collect());
            }
            let mut joined: SmallVec<[PulledSpan<C::Range>; 1]> = SmallVec::new();
            for next in resolved {
                match joined.pop() {
                    Some(previous) if spans_share_a_node(&previous.span, &next.span) => {
                        joined.push(join_overlapping(previous, next));
                    }
                    Some(previous) => {
                        joined.push(previous);
                        joined.push(next);
                    }
                    None => joined.push(next),
                }
            }
            unresolved = joined.into_iter().map(|pulled| pulled.span).collect();
        }
    }

    // resolve_span prepares a span by merging neighbors into it until its entries can fill a node to the minimum.
    // That is only ever needed when all of its entries together would make a single node below the minimum,
    // since anything larger packs into nodes that all meet it.
    async fn resolve_span(
        &self,
        kind: NodeKind,
        mut span: LayerSpan<C::Range>,
    ) -> Result<PulledSpan<C::Range>, TreeFailure<T::Failure, S::Rejection>> {
        if span.is_root() || span.entries.len() == 0 {
            return Ok(PulledSpan::without_pulls(span));
        }
        let mut left_pull_entry_counts = SmallVec::new();
        let mut right_pull_entry_counts = SmallVec::new();
        // check to see if we need to merge with a neighbor to the span
        while Self::node_size(kind, &span.entries, 0..span.entries.len()) < C::MERGE_BELOW_SIZE {
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
                    break;
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
                    left_pull_entry_counts.insert(0, neighbor_entries.len());
                    neighbor_entries.extend_from_list(&span.entries);
                    span.entries = neighbor_entries;
                    span.nodes.insert(0, node);
                }
                Side::Right => {
                    right_pull_entry_counts.push(neighbor_entries.len());
                    span.entries.extend_from_list(&neighbor_entries);
                    span.nodes.push(node);
                }
            }
        }
        Ok(PulledSpan {
            span,
            left_pull_entry_counts,
            right_pull_entry_counts,
        })
    }

    pub(super) fn create_span_mutation_plan(
        &self,
        kind: NodeKind,
        span: IndependentResolvedSpan<C::Range>,
        packing: PackingStrategy,
    ) -> Result<SmallVec<[LayerNodeMutation<C::Range>; 3]>, TreeFailure<T::Failure, S::Rejection>>
    {
        let IndependentResolvedSpan(span) = span;
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
        let optimal_boundaries_plan = packing.boundaries(
            span.entries.len(),
            |range| Self::node_size(kind, &span.entries, range),
            min_node_size,
            C::NODE_CAPACITY,
        );

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

    fn node_size(
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

fn spans_share_a_node<R>(left: &LayerSpan<R>, right: &LayerSpan<R>) -> bool {
    left.nodes.iter().any(|left| {
        right
            .nodes
            .iter()
            .any(|right| left.node.id == right.node.id)
    })
}

fn join_overlapping<R: AddressableRange>(
    previous: PulledSpan<R>,
    next: PulledSpan<R>,
) -> PulledSpan<R> {
    let PulledSpan {
        span: mut joined,
        left_pull_entry_counts,
        right_pull_entry_counts: previous_right_pulls,
    } = previous;
    let PulledSpan {
        span: next,
        left_pull_entry_counts: next_left_pulls,
        right_pull_entry_counts,
    } = next;

    let next_original_first = next.nodes[next_left_pulls.len()].node.id;
    let previous_original_end = joined.nodes.len() - previous_right_pulls.len();
    let gap_from_previous = joined.nodes[previous_original_end..]
        .iter()
        .take_while(|node| node.node.id != next_original_first)
        .count();
    let dropped_entries: usize = previous_right_pulls[gap_from_previous..].iter().sum();
    joined
        .nodes
        .truncate(previous_original_end + gap_from_previous)
        .expect("the previous span keeps its own nodes");
    joined
        .entries
        .truncate(joined.entries.len() - dropped_entries);

    let already_joined = next.nodes[..next_left_pulls.len()]
        .iter()
        .take_while(|node| joined.nodes.iter().any(|kept| kept.node.id == node.node.id))
        .count();
    let skipped_entries: usize = next_left_pulls[..already_joined].iter().sum();
    joined
        .entries
        .extend_from_list_starting_at(&next.entries, skipped_entries);
    joined
        .nodes
        .extend(next.nodes.into_iter().skip(already_joined));
    PulledSpan {
        span: joined,
        left_pull_entry_counts,
        right_pull_entry_counts,
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

#[cfg(test)]
mod tests;
