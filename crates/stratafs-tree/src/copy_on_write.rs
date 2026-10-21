use bytes::{Bytes, BytesMut};
use futures_util::future::{OptionFuture, join};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use zerocopy::little_endian::U128;
use zerocopy::{FromBytes, KnownLayout};

use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeHeader};
use crate::invariants;
use crate::key::TreeKey;
use crate::node::{
    InternalEntry, LEFTMOST_SEPARATOR, LeafEntry, Node, NodeHeader, NodeKind, ObservedNode,
};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::{TreeTransaction, with_tree_failure};

// LayerMutationPlanAction describes a single action that needs to be performed,
// relative to the OBSERVED nodeids. In other words, all nodeids provided here should ALREADY EXIST.
pub(crate) enum LayerMutationPlanAction<R: AddressableRange> {
    Create {
        separator: Box<[u8]>,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Update {
        node: ObservedNode,
        at: usize,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    // Replace is the same as Update, except is makes the parent update its selector to separator
    Replace {
        node: ObservedNode,
        at: usize,
        separator: Box<[u8]>,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Delete {
        node: ObservedNode,
        at: usize,
    },
}

#[derive(Clone, Copy)]
enum NeighborSide {
    Left,
    Right,
}

enum ParentEdit<'plan> {
    Retarget {
        at: usize,
        to: NodeId,
    },
    Insert {
        separator: &'plan [u8],
        child: NodeId,
    },
    Remove {
        at: usize,
    },
    Replace {
        at: usize,
        separator: &'plan [u8],
        child: NodeId,
    },
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

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // write_over handles reconciliation of the desired state in the current volume,
    // against what is currently at the targetted node id.
    // It either replaces the state in place of the leaf,
    // or it iteratively handles copy on write up to the root when necessary.
    // It also handles splitting/merging during the same pass.
    pub(crate) async fn write_over(
        &mut self,
        volume: VolumeHeader,
        steps: DescentSteps<C::Range>,
        node: ObservedNode,
        desired_entries: SlottedList<BytesMut, LeafEntry<C::Range>, C::Range>,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        debug_assert!(invariants::leaf_keys_ascend(&desired_entries));
        self.write_upward(
            volume,
            steps,
            node,
            NodeKind::Leaf,
            desired_entries.into_untyped(),
        )
        .await
    }

    // write_upward is a little helper to remove all the immutable param identifiers from scope, purely for ergonomics.
    async fn write_upward(
        &mut self,
        volume: VolumeHeader,
        mut ancestry: DescentSteps<C::Range>,
        mut writing_over_node: ObservedNode,
        mut writing_over_kind: NodeKind,
        mut entries_to_write: SlottedList<BytesMut, [u8], C::Range>,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        loop {
            let parent = ancestry.pop();
            // no parent in the acestry means our parent is implicitly the volume header, which only has a single slot (so we are at it, being index 0)
            let current_at = parent.as_ref().map_or(0, |parent| parent.position);

            // PHASE 1: construct a plan, in order to maintain node utilization constraints
            let node_size = size_of::<NodeHeader>() + entries_to_write.stored_length();
            let plan: [Option<LayerMutationPlanAction<C::Range>>; 2] = match node_size {
                size if size > C::NODE_CAPACITY => {
                    // handle splitting a node
                    let split_at: usize = entries_to_write.midpoint_by_size();

                    let mut right_half = entries_to_write.split_off(split_at);
                    let separator = separator_for_parent(writing_over_kind, &mut right_half)
                        .map_err(TreeFailure::Corrupt)?;

                    [
                        Some(LayerMutationPlanAction::Update {
                            node: writing_over_node,
                            at: current_at,
                            entries: entries_to_write,
                        }),
                        Some(LayerMutationPlanAction::Create {
                            separator,
                            entries: right_half,
                        }),
                    ]
                }
                size if size < C::MERGE_BELOW_SIZE
                    && let Some(parent) = &parent =>
                {
                    // merge the current node into its least loaded neighbor
                    let left_at = parent.position.checked_sub(1);
                    let right_at =
                        Some(parent.position + 1).filter(|&at| at < parent.entries.len());
                    let inner = &self.inner;
                    let read_neighbor = |side: NeighborSide, at: usize| async move {
                        let id = parent
                            .entries
                            .get(at)
                            .expect("the neighbor's slot is in the parent")
                            .map_err(TreeFailure::Corrupt)?
                            .child;
                        let stored = inner
                            .get(TreeKey::Node(id).into())
                            .await
                            .map_err(TreeFailure::Store)?
                            .ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
                        let (epoch, node) =
                            Node::<Bytes, C::Range>::read(stored).map_err(TreeFailure::Corrupt)?;
                        debug_assert!(matches!(
                            (&node, writing_over_kind),
                            (Node::Leaf(_), NodeKind::Leaf)
                                | (Node::Internal(_), NodeKind::Internal)
                        ));
                        let entries = match node {
                            Node::Leaf(entries) => entries.into_untyped(),
                            Node::Internal(entries) => entries.into_untyped(),
                        };
                        Ok::<_, TreeFailure<T::Failure, S::Rejection>>((
                            side,
                            at,
                            ObservedNode { id, epoch },
                            entries,
                        ))
                    };
                    let (left_neighbor, right_neighbor) = join(
                        OptionFuture::from(left_at.map(|at| read_neighbor(NeighborSide::Left, at))),
                        OptionFuture::from(
                            right_at.map(|at| read_neighbor(NeighborSide::Right, at)),
                        ),
                    )
                    .await;
                    let (side, neighbor_at, neighbor, neighbor_entries) =
                        [left_neighbor.transpose()?, right_neighbor.transpose()?]
                            .into_iter()
                            .flatten()
                            .min_by_key(|(_, _, _, entries)| entries.stored_length())
                            .expect("a node with a parent always has a sibling");

                    // combine the 2 entry lists then split, to evenly distribute the load between them
                    let with_separator_brought_down =
                        |right: SlottedList<BytesMut, [u8], C::Range>, right_at: usize| {
                            match writing_over_kind {
                                NodeKind::Leaf => Ok(right),
                                NodeKind::Internal => {
                                    let separator = &parent
                                        .entries
                                        .get(right_at)
                                        .expect("the right node's slot is in the parent")
                                        .map_err(TreeFailure::Corrupt)?
                                        .key;
                                    let mut right = right.into_typed::<InternalEntry>();
                                    let child = right
                                        .get(0)
                                        .expect("an internal node being merged has a child")
                                        .map_err(TreeFailure::Corrupt)?
                                        .child;
                                    right.replace_entry(0, child, separator);
                                    Ok::<_, TreeFailure<T::Failure, S::Rejection>>(
                                        right.into_untyped(),
                                    )
                                }
                            }
                        };
                    let mut combined_entries = match side {
                        NeighborSide::Left => {
                            let right =
                                with_separator_brought_down(entries_to_write, parent.position)?;
                            let mut combined =
                                SlottedList::<BytesMut, [u8], C::Range>::from(neighbor_entries);
                            combined.extend_from_list(&right);
                            combined
                        }
                        NeighborSide::Right => {
                            let right = with_separator_brought_down(
                                SlottedList::from(neighbor_entries),
                                neighbor_at,
                            )?;
                            entries_to_write.extend_from_list(&right);
                            entries_to_write
                        }
                    };
                    let ((left_node, left_node_at), (right_node, right_node_at)) = match side {
                        NeighborSide::Left => {
                            ((neighbor, neighbor_at), (writing_over_node, current_at))
                        }
                        NeighborSide::Right => {
                            ((writing_over_node, current_at), (neighbor, neighbor_at))
                        }
                    };
                    match size_of::<NodeHeader>() + combined_entries.stored_length() {
                        // redistribute between the 2 nodes
                        size if size > C::NODE_CAPACITY => {
                            let split_at = combined_entries.midpoint_by_size();
                            let mut right_half = combined_entries.split_off(split_at);
                            let separator =
                                separator_for_parent(writing_over_kind, &mut right_half)
                                    .map_err(TreeFailure::Corrupt)?;
                            [
                                Some(LayerMutationPlanAction::Update {
                                    node: left_node,
                                    at: left_node_at,
                                    entries: combined_entries, // after the split off this is now the left half
                                }),
                                Some(LayerMutationPlanAction::Replace {
                                    node: right_node,
                                    at: right_node_at,
                                    separator,
                                    entries: right_half,
                                }),
                            ]
                        }
                        // move all entries into one node
                        _ => [
                            Some(LayerMutationPlanAction::Update {
                                node: left_node,
                                at: left_node_at,
                                entries: combined_entries,
                            }),
                            Some(LayerMutationPlanAction::Delete {
                                node: right_node,
                                at: right_node_at,
                            }),
                        ],
                    }
                }
                _ if writing_over_kind == NodeKind::Internal
                    && parent.is_none()
                    && entries_to_write.len() == 1 =>
                {
                    // handle the root node collapsing (aka when the root nodes children have (over time) merged into 1
                    let only_child = InternalEntry::ref_from_bytes(
                        entries_to_write
                            .get(0)
                            .expect("the list has one entry")
                            .map_err(TreeFailure::Corrupt)?,
                    )
                    .map_err(|_| TreeFailure::Corrupt(CorruptValue::WrongLength))?
                    .child;
                    if writing_over_node.epoch == volume.epoch {
                        self.inner
                            .clear(TreeKey::Node(writing_over_node.id).into())
                            .await
                            .map_err(TreeFailure::Store)?;
                    }
                    self.set_volume_header(VolumeHeader {
                        root: only_child,
                        epoch: volume.epoch,
                    })
                    .await?;
                    return Ok(());
                }
                _ => [
                    Some(LayerMutationPlanAction::Update {
                        node: writing_over_node,
                        at: current_at,
                        entries: entries_to_write,
                    }),
                    None,
                ],
            };

            debug_assert!(invariants::plan_has_at_most_one_action_per_node(&plan));

            // PHASE 2: execute the plan at the current layer, performing copy on write when necessary
            let mut parent_edits: [Option<ParentEdit>; 2] = [None, None];
            for (i, action) in plan.iter().flatten().enumerate() {
                match action {
                    LayerMutationPlanAction::Update { node, at, entries }
                    | LayerMutationPlanAction::Replace {
                        node, at, entries, ..
                    } => {
                        let target = match volume.epoch {
                            current_epoch if current_epoch == node.epoch => node.id,
                            _ => NodeId(U128::new((self.shared.id_allocator)())),
                        };

                        self.inner
                            .set(
                                TreeKey::Node(target).into(),
                                Node::write(
                                    &NodeHeader {
                                        kind: writing_over_kind,
                                        epoch: volume.epoch,
                                    },
                                    entries,
                                ),
                            )
                            .await
                            .map_err(with_tree_failure)?;
                        parent_edits[i] = match action {
                            LayerMutationPlanAction::Replace { separator, .. } => {
                                Some(ParentEdit::Replace {
                                    at: *at,
                                    separator,
                                    child: target,
                                })
                            }
                            _ if node.id != target => Some(ParentEdit::Retarget {
                                at: *at,
                                to: target,
                            }),
                            _ => None,
                        };
                    }
                    LayerMutationPlanAction::Create { separator, entries } => {
                        let target = NodeId(U128::new((self.shared.id_allocator)()));

                        self.inner
                            .set(
                                TreeKey::Node(target).into(),
                                Node::write(
                                    &NodeHeader {
                                        kind: writing_over_kind,
                                        epoch: volume.epoch,
                                    },
                                    entries,
                                ),
                            )
                            .await
                            .map_err(with_tree_failure)?;
                        parent_edits[i] = Some(ParentEdit::Insert {
                            separator,
                            child: target,
                        });
                    }
                    LayerMutationPlanAction::Delete { node, at } => {
                        // in a fork, the deletion only manifests as the parent dereferencing this node, since the previous epoch is frozen
                        if node.epoch == volume.epoch {
                            self.inner
                                .clear(TreeKey::Node(node.id).into())
                                .await
                                .map_err(TreeFailure::Store)?;
                        }

                        parent_edits[i] = Some(ParentEdit::Remove { at: *at });
                    }
                }
            }

            // PHASE 3: mutate the parent's slots to reflect our edits
            if let [None, None] = parent_edits {
                // this layer was editted in place (no nodes were created or removed), no need to continue ascending
                return Ok(());
            }

            // apply the edits to the entries, which will be written to the parent layer next iteration
            let (mut parent_entries, parent_node) = match parent {
                Some(parent) => (
                    SlottedList::<BytesMut, InternalEntry, C::Range>::from(parent.entries),
                    Some(parent.node),
                ),
                None => {
                    let mut header_slot = SlottedList::<BytesMut, InternalEntry, C::Range>::empty();
                    header_slot.insert_entry(0, writing_over_node.id, LEFTMOST_SEPARATOR);
                    (header_slot, None)
                }
            };

            for i in 0..parent_edits.len() {
                fn adjust_future_slots(
                    future_edits: &mut [Option<ParentEdit>],
                    adjust: impl Fn(&mut usize),
                ) {
                    for future_edit in future_edits.iter_mut().flatten() {
                        match future_edit {
                            ParentEdit::Retarget { at, .. }
                            | ParentEdit::Remove { at }
                            | ParentEdit::Replace { at, .. } => adjust(at),
                            ParentEdit::Insert { .. } => {}
                        }
                    }
                }

                let Some(edit) = parent_edits[i].take() else {
                    continue;
                };
                match edit {
                    ParentEdit::Retarget { at, to } => {
                        // retarged the separator on the parent in place
                        parent_entries
                            .get_mut(at)
                            .expect("an entry should be here according to the ancestry")
                            .map_err(TreeFailure::Corrupt)?
                            .child = to
                    }
                    ParentEdit::Insert { separator, child } => {
                        let at = parent_entries
                            .binary_search_by(|entry| Ok(entry.key.cmp(separator)))
                            .map_err(TreeFailure::Corrupt)?
                            .expect_err("a new separator should not already be in the parent");
                        parent_entries.insert_entry(at, child, separator);

                        // now that we have inserted, all future index references need to be incremented +1
                        adjust_future_slots(&mut parent_edits[i + 1..], |future_at| {
                            if *future_at >= at {
                                *future_at += 1;
                            }
                        });
                    }
                    ParentEdit::Replace {
                        at,
                        separator,
                        child,
                    } => parent_entries.replace_entry(at, child, separator),
                    ParentEdit::Remove { at } => {
                        parent_entries.remove(at);
                        adjust_future_slots(&mut parent_edits[i + 1..], |future_at| {
                            debug_assert_ne!(
                                *future_at, at,
                                "a later edit should not target the removed slot"
                            );
                            if *future_at > at {
                                *future_at -= 1;
                            }
                        });
                    }
                }
            }

            // PHASE 4: retarget to the parent so the next iteration updates the layer above this one, or finalize if we are at the root
            match parent_node {
                None => {
                    // if there is no parent, then we are at the root.
                    match parent_entries.len() {
                        1 => {
                            let updated_root = parent_entries
                                .get(0)
                                .expect("length 1 list should have an entry at 0")
                                .map_err(TreeFailure::Corrupt)?;
                            if volume.root == updated_root.child {
                                return Ok(());
                            }

                            // Update the volume to point at the new root
                            self.set_volume_header(VolumeHeader {
                                root: updated_root.child,
                                epoch: volume.epoch,
                            })
                            .await?;
                            return Ok(());
                        }
                        2 => {
                            // create a new root node
                            let new_root = NodeId(U128::new((self.shared.id_allocator)()));
                            self.inner
                                .set(
                                    TreeKey::Node(new_root).into(),
                                    Node::write(
                                        &NodeHeader {
                                            kind: NodeKind::Internal,
                                            epoch: volume.epoch,
                                        },
                                        &parent_entries,
                                    ),
                                )
                                .await
                                .map_err(with_tree_failure)?;
                            self.set_volume_header(VolumeHeader {
                                root: new_root,
                                epoch: volume.epoch,
                            })
                            .await?;
                            return Ok(());
                        }
                        count => unreachable!(
                            "the root never merges, so the header slot has 1 or 2 entries, not {count}"
                        ),
                    }
                }
                Some(parent_node) => {
                    // retarget to write over the parent next
                    writing_over_node = parent_node;
                }
            }
            writing_over_kind = NodeKind::Internal;

            debug_assert!(invariants::separator_keys_ascend(&parent_entries));
            entries_to_write = parent_entries.into_untyped();
        }
    }
}
