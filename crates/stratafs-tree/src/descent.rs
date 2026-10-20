use bytes::Bytes;
use equivalent::Comparable;
use smallvec::SmallVec;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction};

use crate::error::TreeFailure;
use crate::header::NodeId;
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node, ObservedNode};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeConfig;
use crate::volume::TreeTransaction;

const MAX_DEPTH: usize = 64;
const INLINE_DEPTH: usize = 8;

pub(crate) struct DescentStep<R, T: ?Sized, P> {
    pub(crate) node: ObservedNode,
    pub(crate) entries: SlottedList<Bytes, T, R>,
    pub(crate) position: P,
}

pub(crate) type DescentSteps<R> = SmallVec<[DescentStep<R, InternalEntry, usize>; INLINE_DEPTH]>;

pub(crate) struct DescentPath<R: AddressableRange> {
    pub(crate) steps: DescentSteps<R>,
    pub(crate) destination: DescentStep<
        R,
        LeafEntry<R>,
        // leaf entries might not have a match, so this expects the binary search result
        Result<usize, usize>,
    >,
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    C: TreeConfig,
{
    // TODO: these methods assume KV operations are efficiently cached by the underlying kv store,
    // we need to make a wrapper for any generic kv store that does this.
    pub(crate) async fn descend<Q: ?Sized>(
        &self,
        target: &Q,
    ) -> Result<DescentPath<C::Range>, TreeFailure<T::Failure, S::Rejection>>
    where
        [u8]: Comparable<Q>,
    {
        let root = self.get_volume_header().await?.root;
        let mut steps = SmallVec::new();
        let destination = self.descend_from(root, &mut steps, target).await?;
        Ok(DescentPath { steps, destination })
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
            let node_bytes = self
                .inner
                .get(TreeKey::Node(current).into())
                .await?
                .ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
            let (epoch, node) =
                Node::<Bytes, C::Range>::read(node_bytes).map_err(TreeFailure::Corrupt)?;

            match node {
                Node::Leaf(entries) => {
                    let position = entries
                        .binary_search_by(|leaf| {
                            let (leaf_key, _) = leaf.key_and_value()?;
                            Ok(leaf_key.compare(target))
                        })
                        .map_err(TreeFailure::Corrupt)?;
                    return Ok(DescentStep {
                        node: ObservedNode { id: current, epoch },
                        entries,
                        position,
                    });
                }
                Node::Internal(entries) => {
                    let applicable_separator_index = match entries
                        .binary_search_by(|separator| Ok(separator.key.compare(target)))
                        .map_err(TreeFailure::Corrupt)?
                    {
                        Ok(index) => index,
                        Err(0) => {
                            return Err(TreeFailure::Corrupt(
                                CorruptValue::KeyBeforeFirstSeparator,
                            ));
                        }
                        // if there is not an exact match, we are in the applicable range of the separator that is to the left of this index.
                        Err(index) => index - 1,
                    };

                    let this_step_node_id = current;
                    current = entries
                        .get(applicable_separator_index)
                        .expect("the index is in the range")
                        .map_err(TreeFailure::Corrupt)?
                        .child;

                    steps.push(DescentStep {
                        node: ObservedNode {
                            id: this_step_node_id,
                            epoch,
                        },
                        entries,
                        position: applicable_separator_index,
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
