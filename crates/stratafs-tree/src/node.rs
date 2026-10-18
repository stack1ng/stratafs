use bytes::{Bytes, BytesMut};
use zerocopy::little_endian::{U16, U64};
use zerocopy::{
    FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes, TryReadError, Unaligned,
};

use crate::header::NodeId;
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
    pub(crate) epoch: U64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct LeafEntry {
    key_length: U16,
    key_then_value: [u8],
}

impl LeafEntry {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Box<Self> {
        let key_length =
            u16::try_from(key.len()).expect("a leaf entry's key is at most 65535 bytes");
        let mut entry = Self::new_box_zeroed_with_elems(key.len() + value.len())
            .expect("a leaf entry fits in memory");
        entry.key_length = U16::new(key_length);
        let (stored_key, stored_value) = entry.key_then_value.split_at_mut(key.len());
        stored_key.copy_from_slice(key);
        stored_value.copy_from_slice(value);
        entry
    }

    pub(crate) fn key_and_value(&self) -> Result<(&[u8], &[u8]), CorruptValue> {
        self.key_then_value
            .split_at_checked(usize::from(self.key_length.get()))
            .ok_or(CorruptValue::KeyLongerThanEntry)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct InternalEntry {
    pub(crate) child: NodeId,
    pub(crate) key: [u8],
}

pub(crate) enum Node<B, R> {
    Leaf(SlottedList<B, LeafEntry, R>),
    Internal(SlottedList<B, InternalEntry, R>),
}

impl<R: AddressableRange> Node<Bytes, R> {
    pub(crate) fn read(stored: Bytes) -> Result<(NodeHeader, Self), CorruptValue> {
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
        Ok((header, node))
    }

    pub(crate) fn write<T: ?Sized>(
        header: NodeHeader,
        entries: SlottedList<BytesMut, T, R>,
    ) -> Bytes {
        let entries = entries.into_inner();
        let mut stored = BytesMut::with_capacity(size_of::<NodeHeader>() + entries.len());
        stored.extend_from_slice(header.as_bytes());
        stored.extend_from_slice(&entries);
        stored.freeze()
    }
}

#[cfg(test)]
mod tests;
