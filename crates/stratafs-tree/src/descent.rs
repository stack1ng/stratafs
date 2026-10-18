use bytes::Bytes;
use equivalent::Comparable;
use stratafs_store::kv::ReadKeyValueTransaction;

use crate::header::{NodeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::volume::TreeTransaction;

const MAX_DEPTH: usize = 64;

struct DescentStep<R, T: ?Sized, P> {
    entries: SlottedList<Bytes, T, R>,
    epoch: VolumeEpoch,
    position: P,
}

pub(crate) struct DescentPath<R> {
    steps: Vec<DescentStep<R, InternalEntry, usize>>,
    destination: DescentStep<
        R,
        LeafEntry,
        // leaf entries might not have a match, so this expects the binary search result
        Result<usize, usize>,
    >,
}

#[derive(Debug, thiserror::Error)]
pub enum DescentError<F> {
    #[error(transparent)]
    Get(#[from] F),
    #[error(transparent)]
    Corrupt(CorruptValue),
}

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: ReadKeyValueTransaction,
    R: AddressableRange,
{
    // TODO: these methods assume KV operations are effeciently cached by the underlying kv store,
    // we need to make a wrapper for any generic kv store that does this.
    pub(crate) async fn get_volume_header(&self) -> Result<VolumeHeader, DescentError<T::Failure>> {
        let stored = self
            .inner
            .get(TreeKey::VolumeHeader(self.volume).into())
            .await?
            .ok_or(DescentError::Corrupt(CorruptValue::MissingVolumeHeader))?;
        VolumeHeader::view(&stored)
            .copied()
            .map_err(DescentError::Corrupt)
    }

    pub(crate) async fn descend<Q: ?Sized>(
        &self,
        target: &Q,
    ) -> Result<DescentPath<R>, DescentError<T::Failure>>
    where
        [u8]: Comparable<Q>,
    {
        let root = self.get_volume_header().await?.root;
        let mut steps = Vec::new();
        let destination = self.descend_from(root, &mut steps, target).await?;
        Ok(DescentPath { steps, destination })
    }

    async fn descend_from<Q: ?Sized>(
        &self,
        start: NodeId,
        steps: &mut Vec<DescentStep<R, InternalEntry, usize>>,
        target: &Q,
    ) -> Result<DescentStep<R, LeafEntry, Result<usize, usize>>, DescentError<T::Failure>>
    where
        [u8]: Comparable<Q>,
    {
        let mut current = start;
        for _ in steps.len()..MAX_DEPTH {
            let node_bytes = self
                .inner
                .get(TreeKey::Node(current).into())
                .await?
                .ok_or(DescentError::Corrupt(CorruptValue::MissingNode))?;
            let (epoch, node) =
                Node::<Bytes, R>::read(node_bytes).map_err(DescentError::Corrupt)?;

            match node {
                Node::Leaf(entries) => {
                    let position = entries
                        .binary_search_by(|leaf| {
                            let (leaf_key, _) = leaf.key_and_value()?;
                            Ok(leaf_key.compare(target))
                        })
                        .map_err(DescentError::Corrupt)?;
                    return Ok(DescentStep {
                        entries,
                        epoch,
                        position,
                    });
                }
                Node::Internal(entries) => {
                    let applicable_separator_index = match entries
                        .binary_search_by(|separator| Ok(separator.key.compare(target)))
                        .map_err(DescentError::Corrupt)?
                    {
                        Ok(index) => index,
                        Err(0) => {
                            return Err(DescentError::Corrupt(
                                CorruptValue::KeyBeforeFirstSeparator,
                            ));
                        }
                        // if there is not an exact match, we are in the applicable range of the separator that is to the left of this index.
                        Err(index) => index - 1,
                    };

                    current = entries
                        .get(applicable_separator_index)
                        .expect("the index is in the range")
                        .map_err(DescentError::Corrupt)?
                        .child;

                    steps.push(DescentStep {
                        entries,
                        epoch,
                        position: applicable_separator_index,
                    });
                }
            }
        }
        Err(DescentError::Corrupt(CorruptValue::TreeTooDeep))
    }

    // next_entry provides a way to iterate from a DescentPath.
    // If it errors, the cursor should be treated as invalid.
    pub(crate) async fn next_entry<'cursor>(
        &self,
        cursor: &'cursor mut DescentPath<R>,
    ) -> Result<Option<&'cursor LeafEntry>, DescentError<T::Failure>> {
        let next_held_leaf_index = match cursor.destination.position {
            Ok(index) => index,
            Err(next_closest_index) => next_closest_index,
        };
        if next_held_leaf_index >= cursor.destination.entries.len() {
            // we have reached the end of the cursor's held leaf node, so we must find the next
            let mut current_possible_ancestors = &cursor.steps[..];
            loop {
                let Some((ancestor, above)) = current_possible_ancestors.split_last() else {
                    // we are now at the root and could not find anything to the right,
                    // meaning: there is nothing to the right. So we should stop here.
                    return Ok(None);
                };
                if let Some(next) = ancestor.entries.get(ancestor.position + 1) {
                    // we have found a common ancestor between the old position and the next.
                    // now we simply descend from it to its first leaf
                    let next_subtree_root = next.map_err(DescentError::Corrupt)?.child;
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

                    return cursor
                        .destination
                        .entries
                        .get(0)
                        .transpose()
                        .map_err(DescentError::Corrupt);
                }

                // this ancestor has no child to the right, so we must check its parent
                current_possible_ancestors = above;
            }
        }
        match cursor.destination.entries.get(next_held_leaf_index) {
            Some(Ok(entry)) => {
                // we still are iterating on the same held leaf, simply advance the cursor position and return what we got
                cursor.destination.position = Ok(next_held_leaf_index + 1);
                Ok(Some(entry))
            }
            Some(Err(corrupt)) => Err(DescentError::Corrupt(corrupt)),
            None => {
                // TODO: once polonius is stable, we can move the length check branch into here, to remove the redundant length read.
                unreachable!("the position points at an entry in this leaf")
            }
        }
    }
}
