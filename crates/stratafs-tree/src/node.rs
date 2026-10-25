use std::ops::Deref;

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

impl<K: AddressableRange, R: AddressableRange> SlottedList<BytesMut, LeafEntry<K>, R> {
    pub(crate) fn insert_entry(&mut self, index: usize, key: &[u8], value: impl Buf) {
        let key_length = K::encode(key.len()).expect(
            "callers only write keys that fit in a node, and a node fits the addressable range",
        );
        self.insert_zeroed(index, key.len() + value.remaining())
            .fill(key_length, key, value);
    }

    pub(crate) fn replace_entry(&mut self, index: usize, key: &[u8], value: impl Buf) {
        let key_length = K::encode(key.len()).expect(
            "callers only write keys that fit in a node, and a node fits the addressable range",
        );
        self.replace_zeroed(index, key.len() + value.remaining())
            .fill(key_length, key, value);
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct InternalEntry {
    pub(crate) child: NodeId,
    pub(crate) key: [u8],
}

pub(crate) const LEFTMOST_SEPARATOR: &[u8] = b"";

impl InternalEntry {
    fn fill(&mut self, child: NodeId, key: &[u8]) {
        self.child = child;
        self.key.copy_from_slice(key);
    }
}

impl<R: AddressableRange> SlottedList<BytesMut, InternalEntry, R> {
    pub(crate) fn insert_entry(&mut self, index: usize, child: NodeId, key: &[u8]) {
        self.insert_zeroed(index, key.len()).fill(child, key);
    }

    pub(crate) fn replace_entry(&mut self, index: usize, child: NodeId, key: &[u8]) {
        self.replace_zeroed(index, key.len()).fill(child, key);
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

    pub(crate) fn write<B: Deref<Target = [u8]>, T: ?Sized>(
        header: &NodeHeader,
        entries: &SlottedList<B, T, R>,
    ) -> impl Buf + Send {
        header.as_bytes().chain(&entries.get_ref()[..])
    }
}

#[cfg(test)]
mod tests;
