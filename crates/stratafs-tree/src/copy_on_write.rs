use bytes::BytesMut;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use zerocopy::FromBytes;
use zerocopy::little_endian::U128;

use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeHeader};
use crate::invariants;
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node, NodeHeader, NodeKind, ObservedNode};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::{TreeTransaction, with_tree_failure};

// LayerMutationPlanAction describes a single action that needs to be performed,
// relative to the OBSERVED nodeids. In other words, all nodeids provided here should ALREADY EXIST.
pub(crate) enum LayerMutationPlanAction<R: AddressableRange> {
    Create {
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Update {
        node: ObservedNode,
        at: usize,
        entries: SlottedList<BytesMut, [u8], R>,
    },
    Delete {
        node: ObservedNode,
        at: usize,
    },
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
                    todo!("implement split")
                }
                size if size < C::MERGE_BELOW_SIZE && parent.is_some() => {
                    todo!("implement merge")
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
                    LayerMutationPlanAction::Update { node, at, entries } => {
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
                        if node.id != target {
                            parent_edits[i] = Some(ParentEdit::Retarget {
                                at: *at,
                                to: target,
                            })
                        }
                    }
                    LayerMutationPlanAction::Create { entries } => {
                        let target = NodeId(U128::new((self.shared.id_allocator)()));

                        let first_entry = entries
                            .get(0)
                            .expect("expected entries to have at least 1 entry")
                            .map_err(TreeFailure::Corrupt)?;
                        let lower_bound = match writing_over_kind {
                            NodeKind::Leaf => {
                                let (key, _) = LeafEntry::<C::Range>::ref_from_bytes(first_entry)
                                    .map_err(|_| TreeFailure::Corrupt(CorruptValue::WrongLength))?
                                    .key_and_value()
                                    .map_err(TreeFailure::Corrupt)?;
                                key
                            }
                            NodeKind::Internal => {
                                &InternalEntry::ref_from_bytes(first_entry)
                                    .map_err(|_| TreeFailure::Corrupt(CorruptValue::WrongLength))?
                                    .key
                            }
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
                        parent_edits[i] = Some(ParentEdit::Insert {
                            separator: lower_bound,
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
                    header_slot.insert_entry(0, writing_over_node.id, b"");
                    (header_slot, None)
                }
            };

            for edit in parent_edits.into_iter().flatten() {
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
                    }
                    ParentEdit::Remove { at } => parent_entries.remove(at),
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
