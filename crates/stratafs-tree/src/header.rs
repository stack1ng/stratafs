use std::io::Write;

use storekey::{Encode, EncodeError, Writer};
use xxhash_rust::const_xxh32::xxh32;
use zerocopy::little_endian::{U64, U128};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::serialized::{SerializedShape, serialized_value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode)]
pub(crate) struct TreeVolumeId(pub(crate) u128);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(transparent)]
pub(crate) struct NodeId(pub(crate) U128);

impl<F> Encode<F> for NodeId {
    fn encode<W: Write>(&self, writer: &mut Writer<W>) -> Result<(), EncodeError> {
        <u128 as Encode<F>>::encode(&self.0.get(), writer)
    }
}

impl SerializedShape for NodeId {
    const FINGERPRINT: u32 = xxh32(
        &<U128 as SerializedShape>::FINGERPRINT.to_le_bytes(),
        xxh32(b"NodeId", 0),
    );
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(transparent)]
pub(crate) struct VolumeEpoch(pub(crate) U64);

impl SerializedShape for VolumeEpoch {
    const FINGERPRINT: u32 = xxh32(
        &<U64 as SerializedShape>::FINGERPRINT.to_le_bytes(),
        xxh32(b"VolumeEpoch", 0),
    );
}

serialized_value! {
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct VolumeHeader {
        pub(crate) root: NodeId,
        pub(crate) epoch: VolumeEpoch,
    }
}
