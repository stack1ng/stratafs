use bytes::BytesMut;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, SetRejection};
use zerocopy::little_endian::U128;

use crate::descent::DescentStep;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeEpoch, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, Node, NodeHeader, NodeKind};
use crate::slotted::{AddressableRange, SlottedList};
use crate::volume::{TreeTransaction, with_tree_failure};

impl<T, S, A, R, const NODE_CAPACITY: usize> TreeTransaction<'_, T, S, A, R, NODE_CAPACITY>
where
    T: KeyValueTransaction,
    A: Fn() -> u128 + Send + Sync + 'static,
    R: AddressableRange,
{
    // write_over handles reconciliation of the desired state in the current volume,
    // against what is currently at the targetted node id.
    // It either replaces the state in place of the leaf,
    // or it iteratively handles copy on write up to the root when necessary.
    pub(crate) async fn write_over<E: ?Sized>(
        &mut self,
        steps: Vec<DescentStep<R, InternalEntry, usize>>,
        id: NodeId,
        node_epoch: VolumeEpoch,
        kind: NodeKind,
        desired_entries: SlottedList<BytesMut, E, R>,
    ) -> Result<(), StoreError<SetRejection<T::Rejection>, TreeFailure<T::Failure>>> {
        self.write_upward(steps, id, node_epoch, kind, desired_entries.into_untyped())
            .await
    }

    // write_upward is a little helper to remove all the immutable param identifiers from scope, purely for ergonomics.
    async fn write_upward(
        &mut self,
        mut ancestry: Vec<DescentStep<R, InternalEntry, usize>>,
        mut writing_over_id: NodeId,
        mut writing_over_epoch: VolumeEpoch,
        mut writing_over_kind: NodeKind,
        mut entries_to_write: SlottedList<BytesMut, [u8], R>,
    ) -> Result<(), StoreError<SetRejection<T::Rejection>, TreeFailure<T::Failure>>> {
        let volume_epoch = self.get_volume_header().await?.epoch;

        loop {
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
                // this was editted in place, no need to continue recursing
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
            writing_over_id = parent.id;
            writing_over_epoch = parent.epoch;
            writing_over_kind = NodeKind::Internal;

            let mut parent_entries =
                SlottedList::<BytesMut, InternalEntry, R>::from(parent.entries);
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
