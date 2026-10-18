use xxhash_rust::const_xxh32::xxh32;
use zerocopy::byteorder::{ByteOrder, F32, F64, I16, I32, I64, I128, Order, U16, U32, U64, U128};

#[derive(Debug, thiserror::Error)]
pub enum CorruptValue {
    #[error("the stored value was written as a different type")]
    WrongFingerprint,
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored entry positions do not describe the entry data")]
    InvalidEntryPositions,
}

pub(crate) const FINGERPRINT_LENGTH: usize = 4;

pub(crate) trait SerializedShape {
    const FINGERPRINT: u32;
}

impl SerializedShape for u8 {
    const FINGERPRINT: u32 = xxh32(b"u8", 0);
}

impl SerializedShape for i8 {
    const FINGERPRINT: u32 = xxh32(b"i8", 0);
}

const fn byte_order_seed<O: ByteOrder>() -> u32 {
    match O::ORDER {
        Order::BigEndian => 1,
        Order::LittleEndian => 2,
    }
}

macro_rules! byte_order_shapes {
    ($($primitive:ident => $label:literal),* $(,)?) => {
        $(
            impl<O: ByteOrder> SerializedShape for $primitive<O> {
                const FINGERPRINT: u32 = xxh32($label.as_bytes(), byte_order_seed::<O>());
            }
        )*
    };
}

byte_order_shapes! {
    U16 => "u16",
    U32 => "u32",
    U64 => "u64",
    U128 => "u128",
    I16 => "i16",
    I32 => "i32",
    I64 => "i64",
    I128 => "i128",
    F32 => "f32",
    F64 => "f64",
}

impl<T: SerializedShape, const N: usize> SerializedShape for [T; N] {
    const FINGERPRINT: u32 = xxh32(&(N as u64).to_le_bytes(), T::FINGERPRINT);
}

// serialized_value! is a zerocopy wrapper that prepends a fingerprint (which is statically derived by recursively hashing the type's field names and type identifiers) to the serialized format.
// On view, the fingerprint is checked. Apart from the fingerprint prefix, it functions the same as a zerocopy type.
macro_rules! serialized_value {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($field_vis:vis $field:ident: $field_type:ty),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            ::zerocopy::FromBytes,
            ::zerocopy::IntoBytes,
            ::zerocopy::KnownLayout,
            ::zerocopy::Immutable,
            ::zerocopy::Unaligned,
        )]
        #[repr(C)]
        $vis struct $name {
            $($field_vis $field: $field_type),*
        }

        impl $crate::serialized::SerializedShape for $name {
            const FINGERPRINT: u32 = {
                let fingerprint = ::xxhash_rust::const_xxh32::xxh32(stringify!($name).as_bytes(), 0);
                $(
                    let fingerprint =
                        ::xxhash_rust::const_xxh32::xxh32(stringify!($field).as_bytes(), fingerprint);
                    let fingerprint = ::xxhash_rust::const_xxh32::xxh32(
                        &<$field_type as $crate::serialized::SerializedShape>::FINGERPRINT.to_le_bytes(),
                        fingerprint,
                    );
                )*
                fingerprint
            };
        }

        impl $name {
            pub(crate) fn view(stored: &[u8]) -> Result<&Self, $crate::serialized::CorruptValue> {
                let (fingerprint, value) = stored
                    .split_at_checked($crate::serialized::FINGERPRINT_LENGTH)
                    .ok_or($crate::serialized::CorruptValue::WrongLength)?;
                if fingerprint
                    != <Self as $crate::serialized::SerializedShape>::FINGERPRINT.to_le_bytes()
                {
                    return Err($crate::serialized::CorruptValue::WrongFingerprint);
                }
                <Self as ::zerocopy::FromBytes>::ref_from_bytes(value)
                    .map_err(|_| $crate::serialized::CorruptValue::WrongLength)
            }
        }

        impl From<&$name> for ::bytes::Bytes {
            fn from(value: &$name) -> ::bytes::Bytes {
                let value = <$name as ::zerocopy::IntoBytes>::as_bytes(value);
                let mut stored =
                    Vec::with_capacity($crate::serialized::FINGERPRINT_LENGTH + value.len());
                stored.extend_from_slice(
                    &<$name as $crate::serialized::SerializedShape>::FINGERPRINT.to_le_bytes(),
                );
                stored.extend_from_slice(value);
                ::bytes::Bytes::from(stored)
            }
        }
    };
}

pub(crate) use serialized_value;
