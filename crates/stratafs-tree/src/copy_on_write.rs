use bytes::BytesMut;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use zerocopy::little_endian::U128;

use crate::descent::DescentStep;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, LeafEntry, Node, NodeHeader, NodeKind, ObservedNode};
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{TreeConfig, TreeConfigLimits};
use crate::volume::{TreeTransaction, with_tree_failure};

// LayerMutationPlanAction describes a single action that needs to be performed,
// relative to the OBSERVED nodeids. In other words, all nodeids provided here should ALREADY EXIST.
enum LayerMutationPlanAction<R: AddressableRange> {
    Create(SlottedList<BytesMut, [u8], R>),
    Update(ObservedNode, SlottedList<BytesMut, [u8], R>),
    Delete(ObservedNode),
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
        steps: Vec<DescentStep<C::Range, InternalEntry, usize>>,
        node: ObservedNode,
        desired_entries: SlottedList<BytesMut, LeafEntry<C::Range>, C::Range>,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        self.write_upward(
            steps,
            node.id,
            node.epoch,
            NodeKind::Leaf,
            desired_entries.into_untyped(),
        )
        .await
    }

    // write_upward is a little helper to remove all the immutable param identifiers from scope, purely for ergonomics.
    async fn write_upward(
        &mut self,
        mut ancestry: Vec<DescentStep<C::Range, InternalEntry, usize>>,
        mut writing_over_id: NodeId,
        mut writing_over_epoch: VolumeEpoch,
        mut writing_over_kind: NodeKind,
        mut entries_to_write: SlottedList<BytesMut, [u8], C::Range>,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        let volume_epoch = self.get_volume_header().await?.epoch;

        loop {
            let plan = Vec::<LayerMutationPlanAction<C::Range>>::new();
            let node_size = size_of::<NodeHeader>() + entries_to_write.stored_length();
            match node_size {
                size if size > C::NODE_CAPACITY => {
                    todo!("implement split")
                }
                size if size < C::MERGE_BELOW_SIZE && !ancestry.is_empty() => {
                    todo!("implement merge")
                }
                _ => {
                    plan.push(LayerMutationPlanAction::Update(
                        ObservedNode {
                            id: writing_over_id,
                            epoch: writing_over_epoch,
                        },
                        entries_to_write,
                    ));
                }
            }

            let target = match volume_epoch {
                current_epoch if current_epoch == writing_over_epoch => writing_over_id,
                _ => NodeId(U128::new((self.shared.id_allocator)())),
            };

            self.inner
                .set(
                    TreeKey::Node(target).into(),
                    Node::write(
                        NodeHeader {
                            kind: writing_over_kind,
                            epoch: volume_epoch,
                        },
                        entries_to_write,
                    ),
                )
                .await
                .map_err(with_tree_failure)?;
            if target == writing_over_id {
                // this was editted in place, no need to continue ascending
                return Ok(());
            }

            // since we just created a new node, we need to make sure the parent points to it
            let Some(parent) = ancestry.pop() else {
                // if there is no parent, then we just created a new root.
                // Update the volume to point at it
                self.set_volume_header(VolumeHeader {
                    root: target,
                    epoch: volume_epoch,
                })
                .await?;
                return Ok(());
            };

            // retarget to write over the parent internal node
            writing_over_id = parent.node.id;
            writing_over_epoch = parent.node.epoch;
            writing_over_kind = NodeKind::Internal;

            let mut parent_entries =
                SlottedList::<BytesMut, InternalEntry, C::Range>::from(parent.entries);
            // retarged the separator on the parent in place
            parent_entries
                .get_mut(parent.position)
                .expect("an entry should be here according to the ancestry")
                .map_err(TreeFailure::Corrupt)?
                .child = target;
            entries_to_write = parent_entries.into_untyped();
        }
    }
}
