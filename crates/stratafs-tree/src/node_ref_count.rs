use std::cmp::Ordering;
use std::mem;

use bytes::Bytes;
use futures_util::future::try_join_all;
use stratafs_store::content::ContentReference;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, SetRejection};
use stratafs_store::store::Store;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::TreeFailure;
use crate::header::JobId;
use crate::key::{Referenced, TreeKey};
use crate::node::InternalEntry;
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{MetadataRejection, TreeConfig};
use crate::volume::{TreeTransaction, with_tree_failure};

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = MetadataRejection<S>>,
    S: Store,
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
            self.ref_deltas.push((Referenced::Node(child), delta));
        }
        Ok(())
    }

    // push_content_ref_deltas records the content references a leaf span gains and loses. It compares the entries of
    // the span's old leaves, each with whether the volume owns its leaf, with the entries of its new leaves by key, as
    // both are sorted. An entry kept from a copied leaf is one more reference, as is a new entry of an existing blob,
    // and an entry an owned leaf loses is one less. A new blob's entry is the reference a missing count stands for.
    pub(crate) fn push_content_ref_deltas<'entries>(
        &mut self,
        mut old: impl Iterator<Item = Result<(bool, (&'entries [u8], &'entries [u8])), CorruptValue>>,
        mut new: impl Iterator<Item = Result<(&'entries [u8], &'entries [u8]), CorruptValue>>,
    ) -> Result<(), CorruptValue> {
        let (mut old_entry, mut new_entry) = (old.next().transpose()?, new.next().transpose()?);
        loop {
            let ordering = match (old_entry, new_entry) {
                (None, None) => return Ok(()),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((_, (old_key, _))), Some((new_key, _))) => old_key.cmp(new_key),
            };
            let kept = old_entry.map(|(_, entry)| entry) == new_entry;
            if ordering.is_le() {
                let (owned, (key, value)) = old_entry.expect("the old entry sorts first");
                let delta = match (owned, kept) {
                    (true, false) => -1,
                    (false, true) => 1,
                    (true, true) | (false, false) => 0,
                };
                if delta != 0
                    && let Some(reference) = C::referenced_content(key, value)
                {
                    self.ref_deltas
                        .push((Referenced::Content(reference.id().into()), delta));
                }
                old_entry = old.next().transpose()?;
            }
            if ordering.is_ge() {
                let (key, value) = new_entry.expect("the new entry sorts first");
                if !kept
                    && let Some(ContentReference::Existing(id)) = C::referenced_content(key, value)
                {
                    self.ref_deltas.push((Referenced::Content(id.into()), 1));
                }
                new_entry = new.next().transpose()?;
            }
        }
    }

    // apply_ref_deltas nets the write's changes per node and blob, so a pointer that only moved costs nothing,
    // then applies them, reading the counts that grow together.
    // A count that drops is left to the garbage collector, as one blind job write per lost pointer.
    pub(crate) async fn apply_ref_deltas(
        &mut self,
    ) -> Result<
        (),
        StoreError<
            SetRejection<MetadataRejection<S>>,
            TreeFailure<T::Failure, MetadataRejection<S>>,
        >,
    > {
        let mut deltas = mem::take(&mut self.ref_deltas);
        deltas.sort_unstable_by_key(|(referenced, _)| *referenced);
        deltas.dedup_by(|(referenced, delta), (kept, kept_delta)| {
            let same = referenced == kept;
            if same {
                *kept_delta += *delta;
            }
            same
        });
        debug_assert!(
            deltas
                .iter()
                .all(|(referenced, delta)| *delta >= -1
                    || matches!(referenced, Referenced::Content(_))),
            "a write changes one volume's tree, where a node has one parent or is the root"
        );
        for (referenced, delta) in deltas.iter().filter(|(_, delta)| *delta < 0) {
            for _ in 0..delta.unsigned_abs() {
                let job = JobId((self.shared.id_allocator)());
                self.inner
                    .set(
                        Bytes::from(TreeKey::Dereference(self.volume, *referenced, job)),
                        Bytes::new(),
                    )
                    .map_err(with_tree_failure)?;
            }
        }
        deltas.retain(|(_, delta)| *delta > 0);
        if !deltas.is_empty() {
            let stored = try_join_all(deltas.iter().map(|(referenced, _)| {
                self.inner.get(Bytes::from(TreeKey::RefCount(*referenced)))
            }))
            .await
            .map_err(|failure| StoreError::Failed(TreeFailure::Store(failure)))?;
            for ((referenced, delta), stored) in deltas.iter().zip(stored) {
                let count = stored_count(stored.as_deref())
                    .map_err(|corrupt| StoreError::Failed(TreeFailure::Corrupt(corrupt)))?;
                self.inner
                    .set(
                        Bytes::from(TreeKey::RefCount(*referenced)),
                        U64::new(count + *delta as u64).as_bytes(),
                    )
                    .map_err(with_tree_failure)?;
            }
        }
        deltas.clear();
        self.ref_deltas = deltas;
        Ok(())
    }
}

// stored_count is a reference count as stored, where a missing count is 1.
pub(crate) fn stored_count(stored: Option<&[u8]>) -> Result<u64, CorruptValue> {
    match stored {
        None => Ok(1),
        Some(stored) => U64::read_from_bytes(stored)
            .map(|count| count.get())
            .map_err(|_| CorruptValue::WrongLength),
    }
}
