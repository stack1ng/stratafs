use std::mem;

use bytes::Bytes;
use futures_util::future::try_join_all;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::TreeFailure;
use crate::key::TreeKey;
use crate::node::InternalEntry;
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeConfig;
use crate::volume::{TreeTransaction, with_tree_failure};

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // push_child_ref_deltas records delta for each child pointer in an internal node's entries.
    pub(crate) fn push_child_ref_deltas<B: AsRef<[u8]>, R: AddressableRange>(
        &mut self,
        entries: &SlottedList<B, [u8], R>,
        delta: i64,
    ) -> Result<(), CorruptValue> {
        for index in 0..entries.len() {
            let entry = entries.get(index).expect("the index is in the list")?;
            let child = InternalEntry::ref_from_bytes(entry)
                .map_err(|_| CorruptValue::WrongLength)?
                .child;
            self.node_ref_deltas.push((child, delta));
        }
        Ok(())
    }

    // apply_node_ref_deltas nets the write's changes per node, so a pointer that only moved costs nothing,
    // then applies them, reading the counts that grow together.
    pub(crate) async fn apply_node_ref_deltas(
        &mut self,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        let mut deltas = mem::take(&mut self.node_ref_deltas);
        deltas.sort_unstable_by_key(|(id, _)| id.0.get());
        deltas.dedup_by(|(id, delta), (kept_id, kept_delta)| {
            let same = id == kept_id;
            if same {
                *kept_delta += *delta;
            }
            same
        });
        if deltas.iter().any(|(_, delta)| *delta < 0) {
            todo!("handle node ref decrement")
        }
        deltas.retain(|(_, delta)| *delta > 0);
        if !deltas.is_empty() {
            let stored = try_join_all(
                deltas
                    .iter()
                    .map(|(id, _)| self.inner.get(Bytes::from(TreeKey::NodeRefCount(*id)))),
            )
            .await
            .map_err(|failure| StoreError::Failed(TreeFailure::Store(failure)))?;
            for ((id, delta), stored) in deltas.iter().zip(stored) {
                let count = stored_count(stored.as_deref())
                    .map_err(|corrupt| StoreError::Failed(TreeFailure::Corrupt(corrupt)))?;
                self.inner
                    .set(
                        Bytes::from(TreeKey::NodeRefCount(*id)),
                        U64::new(count + *delta as u64).as_bytes(),
                    )
                    .map_err(with_tree_failure)?;
            }
        }
        deltas.clear();
        self.node_ref_deltas = deltas;
        Ok(())
    }
}

// stored_count is a node's reference count as stored, where a missing count is 1.
fn stored_count(stored: Option<&[u8]>) -> Result<u64, CorruptValue> {
    match stored {
        None => Ok(1),
        Some(stored) => U64::read_from_bytes(stored)
            .map(|count| count.get())
            .map_err(|_| CorruptValue::WrongLength),
    }
}
