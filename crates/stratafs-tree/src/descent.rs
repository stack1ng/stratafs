use std::ops::Range;
use std::slice;

use bytes::Bytes;
use equivalent::Comparable;
use futures_util::future::try_join_all;
use smallvec::SmallVec;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction};

use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node, ObservedNode};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeConfig;
use crate::volume::TreeTransaction;

const MAX_DEPTH: usize = 64;
const INLINE_DEPTH: usize = 4;

pub(crate) struct DescentStep<R, T: ?Sized, P> {
    pub(crate) node: ObservedNode,
    pub(crate) entries: SlottedList<Bytes, T, R>,
    pub(crate) position: P,
}

impl<R, T: ?Sized, P: Clone> Clone for DescentStep<R, T, P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node,
            entries: self.entries.clone(),
            position: self.position.clone(),
        }
    }
}

pub(crate) type DescentSteps<R> = SmallVec<[DescentStep<R, InternalEntry, usize>; INLINE_DEPTH]>;

pub(crate) struct DescentPath<R: AddressableRange> {
    pub(crate) volume: VolumeHeader,
    pub(crate) steps: DescentSteps<R>,
    pub(crate) destination: DescentStep<
        R,
        LeafEntry<R>,
        // leaf entries might not have a match, so this expects the binary search result
        Result<usize, usize>,
    >,
}

// InProgressDescent is a path down the tree that some of the targets still share,
// waiting for its next node to be read.
struct InProgressDescent<R> {
    steps: DescentSteps<R>,
    node: NodeId,
    // the targets heading to node, as a range of the sorted targets
    targets: Range<usize>,
}

fn leaf_position<R: AddressableRange, Q: ?Sized>(
    entries: &SlottedList<Bytes, LeafEntry<R>, R>,
    target: &Q,
) -> Result<Result<usize, usize>, CorruptValue>
where
    [u8]: Comparable<Q>,
{
    entries.binary_search_by(|leaf| {
        let (leaf_key, _) = leaf.key_and_value()?;
        Ok(leaf_key.compare(target))
    })
}

fn child_position<R: AddressableRange, Q: ?Sized>(
    entries: &SlottedList<Bytes, InternalEntry, R>,
    target: &Q,
) -> Result<usize, CorruptValue>
where
    [u8]: Comparable<Q>,
{
    match entries.binary_search_by(|separator| Ok(separator.key.compare(target)))? {
        Ok(index) => Ok(index),
        Err(0) => Err(CorruptValue::KeyBeforeFirstSeparator),
        // if there is not an exact match, we are in the applicable range of the separator that is to the left of this index.
        Err(index) => Ok(index - 1),
    }
}

impl<R: AddressableRange> DescentStep<R, LeafEntry<R>, Result<usize, usize>> {
    pub(crate) fn found_value(&self) -> Result<Option<&[u8]>, CorruptValue> {
        let Ok(index) = self.position else {
            return Ok(None);
        };
        let entry = self
            .entries
            .get(index)
            .expect("the descent found the key at this index")?;
        let (_, value) = entry.key_and_value()?;
        Ok(Some(value))
    }
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    C: TreeConfig,
{
    pub(crate) async fn descend<Q>(
        &self,
        target: &Q,
    ) -> Result<DescentPath<C::Range>, TreeFailure<T::Failure, S::Rejection>>
    where
        Q: Ord,
        [u8]: Comparable<Q>,
    {
        let mut paths = self.descend_many(slice::from_ref(target)).await?;
        Ok(paths.pop().expect("one target descends to one path"))
    }

    // descend_many descends to every target at once, reading each level's nodes in parallel,
    // and returns one path per target, in the same order as the targets.
    // Targets should be sorted so the ones sharing a node share its read. Unsorted targets still get
    // correct paths, but may read the same node more than once.
    pub(crate) async fn descend_many<Q>(
        &self,
        targets: &[Q],
    ) -> Result<SmallVec<[DescentPath<C::Range>; 1]>, TreeFailure<T::Failure, S::Rejection>>
    where
        Q: Ord,
        [u8]: Comparable<Q>,
    {
        debug_assert!(targets.is_sorted(), "descend_many takes sorted targets");
        let volume = self.get_volume_header().await?;
        let mut paths = SmallVec::with_capacity(targets.len());
        let mut in_progress: SmallVec<[InProgressDescent<C::Range>; 1]> = SmallVec::new();
        if !targets.is_empty() {
            in_progress.push(InProgressDescent {
                steps: SmallVec::new(),
                node: volume.root,
                targets: 0..targets.len(),
            });
        }
        for _ in 0..MAX_DEPTH {
            if in_progress.is_empty() {
                return Ok(paths);
            }
            let nodes = try_join_all(
                in_progress
                    .iter()
                    .map(|descent| self.read_node(descent.node)),
            )
            .await?;
            let mut next = SmallVec::new();
            for (descent, (epoch, node)) in in_progress.into_iter().zip(nodes) {
                let observed = ObservedNode {
                    id: descent.node,
                    epoch,
                };
                match node {
                    Node::Leaf(entries) => {
                        for target in &targets[descent.targets] {
                            let position =
                                leaf_position(&entries, target).map_err(TreeFailure::Corrupt)?;
                            paths.push(DescentPath {
                                volume,
                                steps: descent.steps.clone(),
                                destination: DescentStep {
                                    node: observed,
                                    entries: entries.clone(),
                                    position,
                                },
                            });
                        }
                    }
                    Node::Internal(entries) => {
                        for index in descent.targets {
                            let position = child_position(&entries, &targets[index])
                                .map_err(TreeFailure::Corrupt)?;
                            match next.last_mut() {
                                Some(InProgressDescent {
                                    steps,
                                    targets: grouped,
                                    ..
                                }) if steps.last().is_some_and(
                                    |step: &DescentStep<_, _, _>| {
                                        step.node == observed && step.position == position
                                    },
                                ) =>
                                {
                                    grouped.end = index + 1;
                                }
                                _ => {
                                    let mut steps = descent.steps.clone();
                                    steps.push(DescentStep {
                                        node: observed,
                                        entries: entries.clone(),
                                        position,
                                    });
                                    next.push(InProgressDescent {
                                        steps,
                                        node: entries
                                            .get(position)
                                            .expect("the position is in the node")
                                            .map_err(TreeFailure::Corrupt)?
                                            .child,
                                        targets: index..index + 1,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            in_progress = next;
        }
        Err(TreeFailure::Corrupt(CorruptValue::TreeTooDeep))
    }

    async fn read_node(
        &self,
        id: NodeId,
    ) -> Result<(VolumeEpoch, Node<Bytes, C::Range>), TreeFailure<T::Failure, S::Rejection>> {
        let stored = self
            .inner
            .get(TreeKey::Node(id).into())
            .await?
            .ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
        Node::read(stored).map_err(TreeFailure::Corrupt)
    }

    async fn descend_from<Q: ?Sized>(
        &self,
        start: NodeId,
        steps: &mut DescentSteps<C::Range>,
        target: &Q,
    ) -> Result<
        DescentStep<C::Range, LeafEntry<C::Range>, Result<usize, usize>>,
        TreeFailure<T::Failure, S::Rejection>,
    >
    where
        [u8]: Comparable<Q>,
    {
        let mut current = start;
        for _ in steps.len()..MAX_DEPTH {
            let (epoch, node) = self.read_node(current).await?;
            let observed = ObservedNode { id: current, epoch };
            match node {
                Node::Leaf(entries) => {
                    let position = leaf_position(&entries, target).map_err(TreeFailure::Corrupt)?;
                    return Ok(DescentStep {
                        node: observed,
                        entries,
                        position,
                    });
                }
                Node::Internal(entries) => {
                    let position =
                        child_position(&entries, target).map_err(TreeFailure::Corrupt)?;
                    current = entries
                        .get(position)
                        .expect("the position is in the node")
                        .map_err(TreeFailure::Corrupt)?
                        .child;
                    steps.push(DescentStep {
                        node: observed,
                        entries,
                        position,
                    });
                }
            }
        }
        Err(TreeFailure::Corrupt(CorruptValue::TreeTooDeep))
    }

    // next_entry provides a way to iterate from a DescentPath.
    // If it errors or is cancelled, the cursor should be treated as invalid.
    // It returns the next leaf entry and its backing buffer.
    pub(crate) async fn next_entry<'cursor>(
        &self,
        cursor: &'cursor mut DescentPath<C::Range>,
    ) -> Result<
        Option<(&'cursor LeafEntry<C::Range>, &'cursor Bytes)>,
        TreeFailure<T::Failure, S::Rejection>,
    > {
        let next_held_leaf_index = match cursor.destination.position {
            Ok(index) => index,
            Err(next_closest_index) => next_closest_index,
        };
        if next_held_leaf_index >= cursor.destination.entries.len() {
            // we have reached the end of the cursor's held leaf node, so we must find the next leaf node
            let mut current_possible_ancestors = &cursor.steps[..];
            loop {
                let Some((ancestor, above)) = current_possible_ancestors.split_last() else {
                    // we are now at the root and could not find anything to the right,
                    // meaning: there is nothing to the right. So we should stop here.
                    return Ok(None);
                };
                if let Some(next) = ancestor.entries.get(ancestor.position + 1) {
                    // we have found a common ancestor between the old position and the next.
                    // now we simply descend from the common ancestor's next child to that subtree's first leaf
                    let next_subtree_root = next.map_err(TreeFailure::Corrupt)?.child;
                    cursor.steps.truncate(current_possible_ancestors.len());

                    let common_ancestor = cursor
                        .steps
                        .last_mut()
                        .expect("the common ancestor is the last step after truncating");
                    // move the cursor onto the next_subtree that we just discovered
                    common_ancestor.position += 1;

                    // descend to the leftmost part of the next_subtree that we just discovered
                    let next_descent = self
                        .descend_from(
                            next_subtree_root,
                            &mut cursor.steps,
                            // target the first entry within this neighboring subtree
                            &b""[..],
                        )
                        .await?;
                    cursor.destination = next_descent;
                    // ensure that we are pointing directly after the first entry
                    cursor.destination.position = Ok(1);

                    let entries = &cursor.destination.entries;
                    return Ok(entries
                        .get(0)
                        .transpose()
                        .map_err(TreeFailure::Corrupt)?
                        .map(|entry| (entry, entries.get_ref())));
                }

                // this ancestor has no child to the right, so we must check its parent
                current_possible_ancestors = above;
            }
        }
        match cursor.destination.entries.get(next_held_leaf_index) {
            Some(Ok(entry)) => {
                // we still are iterating on the same held leaf, simply advance the cursor position and return what we got
                cursor.destination.position = Ok(next_held_leaf_index + 1);
                Ok(Some((entry, cursor.destination.entries.get_ref())))
            }
            Some(Err(corrupt)) => Err(TreeFailure::Corrupt(corrupt)),
            None => {
                // TODO: once polonius is stable, we can move the length check branch into here, to remove the redundant length read.
                unreachable!("the position points at an entry in this leaf")
            }
        }
    }
}

#[cfg(test)]
mod tests;
