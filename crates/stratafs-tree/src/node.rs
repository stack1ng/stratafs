use bytes::{Buf, Bytes, BytesMut};
use zerocopy::{
    FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes, TryReadError, Unaligned,
};

use crate::header::{NodeId, VolumeEpoch};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(u8)]
pub(crate) enum NodeKind {
    Leaf = 0,
    Internal = 1,
}

#[derive(Debug, Clone, Copy, TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct NodeHeader {
    pub(crate) kind: NodeKind,
    pub(crate) epoch: VolumeEpoch,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct LeafEntry<R: AddressableRange> {
    key_length: R::Stored,
    key_then_value: [u8],
}

impl<R: AddressableRange> LeafEntry<R> {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Option<Box<Self>> {
        let key_length = R::encode(key.len())?;
        let mut entry = Self::new_box_zeroed_with_elems(key.len() + value.len())
            .expect("a leaf entry fits in memory");
        entry.fill(key_length, key, value);
        Some(entry)
    }

    fn fill(&mut self, key_length: R::Stored, key: &[u8], mut value: impl Buf) {
        self.key_length = key_length;
        let (stored_key, stored_value) = self.key_then_value.split_at_mut(key.len());
        stored_key.copy_from_slice(key);
        value.copy_to_slice(stored_value);
    }

    pub(crate) fn key_and_value(&self) -> Result<(&[u8], &[u8]), CorruptValue> {
        self.key_then_value
            .split_at_checked(R::decode(&self.key_length))
            .ok_or(CorruptValue::KeyLongerThanEntry)
    }
}

impl<R: AddressableRange> SlottedList<BytesMut, LeafEntry<R>, R> {
    pub(crate) fn insert_entry(&mut self, index: usize, key: &[u8], value: impl Buf) -> Option<()> {
        let key_length = R::encode(key.len())?;
        self.insert_zeroed(index, key.len() + value.remaining())
            .fill(key_length, key, value);
        Some(())
    }

    pub(crate) fn replace_entry(
        &mut self,
        index: usize,
        key: &[u8],
        value: impl Buf,
    ) -> Option<()> {
        let key_length = R::encode(key.len())?;
        self.replace_zeroed(index, key.len() + value.remaining())
            .fill(key_length, key, value);
        Some(())
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct InternalEntry {
    pub(crate) child: NodeId,
    pub(crate) key: [u8],
}

impl<R: AddressableRange> SlottedList<BytesMut, InternalEntry, R> {
    pub(crate) fn insert_entry(&mut self, index: usize, child: NodeId, key: &[u8]) {
        let entry = self.insert_zeroed(index, key.len());
        entry.child = child;
        entry.key.copy_from_slice(key);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedNode {
    pub(crate) id: NodeId,
    pub(crate) epoch: VolumeEpoch,
}

pub(crate) enum Node<B, R: AddressableRange> {
    Leaf(SlottedList<B, LeafEntry<R>, R>),
    Internal(SlottedList<B, InternalEntry, R>),
}

impl<R: AddressableRange> Node<Bytes, R> {
    pub(crate) fn read(stored: Bytes) -> Result<(VolumeEpoch, Self), CorruptValue> {
        let (header, rest) =
            NodeHeader::try_read_from_prefix(&stored).map_err(|error| match error {
                TryReadError::Size(_) => CorruptValue::WrongLength,
                TryReadError::Validity(_) => CorruptValue::UnknownNodeKind,
            })?;
        let entries = stored.slice_ref(rest);
        let node = match header.kind {
            NodeKind::Leaf => Self::Leaf(SlottedList::new(entries)?),
            NodeKind::Internal => Self::Internal(SlottedList::new(entries)?),
        };
        Ok((header.epoch, node))
    }

    pub(crate) fn write<T: ?Sized>(
        header: NodeHeader,
        entries: &SlottedList<BytesMut, T, R>,
    ) -> Bytes {
        let entries = entries.get_ref();
        let mut stored = BytesMut::with_capacity(size_of::<NodeHeader>() + entries.len());
        stored.extend_from_slice(header.as_bytes());
        stored.extend_from_slice(entries);
        stored.freeze()
    }
}

#[cfg(test)]
mod tests;
