use xxhash_rust::const_xxh32::xxh32;
use zerocopy::byteorder::{ByteOrder, F32, F64, I16, I32, I64, I128, Order, U16, U32, U64, U128};

#[derive(Debug, thiserror::Error)]
pub enum ViewError {
    #[error("the stored value was written as a different type")]
    WrongFingerprint,
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored value holds a value its type does not allow")]
    Invalid,
}

// LittleEndian is a type's little endian form: the same value with every integer in little endian byte order, which
// has no gaps between its fields, so it is stored as its bytes and read in place.
pub trait LittleEndian: Sized {
    type LittleEndian: SerializedShape
        + zerocopy::FromBytes
        + zerocopy::IntoBytes
        + zerocopy::KnownLayout
        + zerocopy::Immutable
        + zerocopy::Unaligned;

    fn to_little_endian(&self) -> Self::LittleEndian;

    // from_little_endian fails when the little endian form holds a value the type does not allow.
    fn from_little_endian(little_endian: &Self::LittleEndian) -> Result<Self, ViewError>;
}

impl LittleEndian for u8 {
    type LittleEndian = u8;

    fn to_little_endian(&self) -> u8 {
        *self
    }

    fn from_little_endian(little_endian: &u8) -> Result<Self, ViewError> {
        Ok(*little_endian)
    }
}

macro_rules! little_endian_integers {
    ($($plain:ident => $little_endian:ident),* $(,)?) => {
        $(
            impl LittleEndian for $plain {
                type LittleEndian = zerocopy::little_endian::$little_endian;

                fn to_little_endian(&self) -> Self::LittleEndian {
                    zerocopy::little_endian::$little_endian::new(*self)
                }

                fn from_little_endian(little_endian: &Self::LittleEndian) -> Result<Self, ViewError> {
                    Ok(little_endian.get())
                }
            }
        )*
    };
}

little_endian_integers! {
    u16 => U16,
    u32 => U32,
    u64 => U64,
    u128 => U128,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    i128 => I128,
}

// little_endian_newtypes! gives each type that wraps one value, such as a branded integer, the little endian form of
// that value.
#[macro_export]
macro_rules! little_endian_newtypes {
    ($($name:ident($value:ty)),* $(,)?) => {
        $(
            impl $crate::LittleEndian for $name {
                type LittleEndian = <$value as $crate::LittleEndian>::LittleEndian;

                fn to_little_endian(&self) -> Self::LittleEndian {
                    $crate::LittleEndian::to_little_endian(&self.0)
                }

                fn from_little_endian(
                    little_endian: &Self::LittleEndian,
                ) -> Result<Self, $crate::ViewError> {
                    $crate::LittleEndian::from_little_endian(little_endian).map($name)
                }
            }
        )*
    };
}

pub const FINGERPRINT_LENGTH: usize = 4;

pub trait SerializedShape {
    const FINGERPRINT: u32;
}

pub const fn fingerprint(bytes: &[u8], seed: u32) -> u32 {
    xxh32(bytes, seed)
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
#[macro_export]
macro_rules! serialized_value {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident as $little_endian:ident {
            $($field_vis:vis $field:ident: $field_type:ty),* $(,)?
        }
    ) => {
        $crate::serialized_shape! {
            $(#[$meta])*
            $vis struct $name as $little_endian {
                $($field_vis $field: $field_type),*
            }
        }

        impl $name {
            pub(crate) fn view(stored: &[u8]) -> Result<Self, $crate::ViewError> {
                let (fingerprint, value) = stored
                    .split_at_checked($crate::FINGERPRINT_LENGTH)
                    .ok_or($crate::ViewError::WrongLength)?;
                if fingerprint
                    != <$little_endian as $crate::SerializedShape>::FINGERPRINT.to_le_bytes()
                {
                    return Err($crate::ViewError::WrongFingerprint);
                }
                let little_endian = <$little_endian as ::zerocopy::FromBytes>::ref_from_bytes(value)
                    .map_err(|_| $crate::ViewError::WrongLength)?;
                $crate::LittleEndian::from_little_endian(little_endian)
            }
        }

        impl From<&$name> for ::bytes::Bytes {
            fn from(value: &$name) -> ::bytes::Bytes {
                let little_endian = $crate::LittleEndian::to_little_endian(value);
                let little_endian = <$little_endian as ::zerocopy::IntoBytes>::as_bytes(&little_endian);
                let mut stored =
                    Vec::with_capacity($crate::FINGERPRINT_LENGTH + little_endian.len());
                stored.extend_from_slice(
                    &<$little_endian as $crate::SerializedShape>::FINGERPRINT.to_le_bytes(),
                );
                stored.extend_from_slice(little_endian);
                ::bytes::Bytes::from(stored)
            }
        }
    };
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

        impl $crate::SerializedShape for $name {
            const FINGERPRINT: u32 = {
                let fingerprint = $crate::fingerprint(stringify!($name).as_bytes(), 0);
                $(
                    let fingerprint =
                        $crate::fingerprint(stringify!($field).as_bytes(), fingerprint);
                    let fingerprint = $crate::fingerprint(
                        &<$field_type as $crate::SerializedShape>::FINGERPRINT.to_le_bytes(),
                        fingerprint,
                    );
                )*
                fingerprint
            };
        }

        impl $name {
            pub(crate) fn view(stored: &[u8]) -> Result<&Self, $crate::ViewError> {
                let (fingerprint, value) = stored
                    .split_at_checked($crate::FINGERPRINT_LENGTH)
                    .ok_or($crate::ViewError::WrongLength)?;
                if fingerprint
                    != <Self as $crate::SerializedShape>::FINGERPRINT.to_le_bytes()
                {
                    return Err($crate::ViewError::WrongFingerprint);
                }
                <Self as ::zerocopy::FromBytes>::ref_from_bytes(value)
                    .map_err(|_| $crate::ViewError::WrongLength)
            }
        }

        impl From<&$name> for ::bytes::Bytes {
            fn from(value: &$name) -> ::bytes::Bytes {
                let value = <$name as ::zerocopy::IntoBytes>::as_bytes(value);
                let mut stored =
                    Vec::with_capacity($crate::FINGERPRINT_LENGTH + value.len());
                stored.extend_from_slice(
                    &<$name as $crate::SerializedShape>::FINGERPRINT.to_le_bytes(),
                );
                stored.extend_from_slice(value);
                ::bytes::Bytes::from(stored)
            }
        }
    };
}

// serialized_shape! declares a struct as written, and its little endian form under the second name, with the same
// fields in little endian form. The little endian form gets the fingerprint, statically derived by recursively hashing
// the type's field names and type identifiers.
#[macro_export]
macro_rules! serialized_shape {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident as $little_endian:ident {
            $($field_vis:vis $field:ident: $field_type:ty),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $($field_vis $field: $field_type),*
        }

        #[derive(
            ::zerocopy::FromBytes,
            ::zerocopy::IntoBytes,
            ::zerocopy::KnownLayout,
            ::zerocopy::Immutable,
            ::zerocopy::Unaligned,
        )]
        #[repr(C)]
        $vis struct $little_endian {
            $($field_vis $field: <$field_type as $crate::LittleEndian>::LittleEndian),*
        }

        impl $crate::SerializedShape for $little_endian {
            const FINGERPRINT: u32 = {
                let fingerprint = $crate::fingerprint(stringify!($name).as_bytes(), 0);
                $(
                    let fingerprint =
                        $crate::fingerprint(stringify!($field).as_bytes(), fingerprint);
                    let fingerprint = $crate::fingerprint(
                        &<<$field_type as $crate::LittleEndian>::LittleEndian as $crate::SerializedShape>::FINGERPRINT
                            .to_le_bytes(),
                        fingerprint,
                    );
                )*
                fingerprint
            };
        }

        impl $crate::LittleEndian for $name {
            type LittleEndian = $little_endian;

            fn to_little_endian(&self) -> $little_endian {
                $little_endian {
                    $($field: $crate::LittleEndian::to_little_endian(&self.$field)),*
                }
            }

            fn from_little_endian(little_endian: &$little_endian) -> Result<Self, $crate::ViewError> {
                Ok($name {
                    $($field: $crate::LittleEndian::from_little_endian(&little_endian.$field)?),*
                })
            }
        }
    };
}
