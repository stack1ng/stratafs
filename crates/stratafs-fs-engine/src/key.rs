use std::borrow::Cow;
use std::ops::Bound;

use bytes::Bytes;
use storekey::{BorrowDecode, Encode};

use crate::Inode;
use crate::error::CorruptValue;

#[derive(Encode, BorrowDecode)]
pub(crate) struct FsKey<'a> {
    // the inode goes here first to group the inode's keys together lexicographically
    pub(crate) inode: Inode,
    pub(crate) kind: KeyKind<'a>,
}

#[derive(Encode, BorrowDecode)]
pub(crate) enum KeyKind<'a> {
    Attributes,
    ExtendedAttribute(Cow<'a, [u8]>),
}

impl<'a> FsKey<'a> {
    pub(crate) fn new_attributes(inode: Inode) -> Self {
        FsKey {
            inode,
            kind: KeyKind::Attributes,
        }
    }

    pub(crate) fn new_extended_attribute(inode: Inode, name: &'a [u8]) -> Self {
        FsKey {
            inode,
            kind: KeyKind::ExtendedAttribute(Cow::Borrowed(name)),
        }
    }
}

impl From<FsKey<'_>> for Bytes {
    fn from(key: FsKey<'_>) -> Bytes {
        Bytes::from(
            storekey::encode_vec(&key)
                .expect("a derived encoding only fails on custom errors, and FsKey raises none"),
        )
    }
}

pub(crate) fn extended_attributes_range(inode: Inode) -> (Bound<Bytes>, Bound<Bytes>) {
    let empty_name = Bytes::from(FsKey::new_extended_attribute(inode, &[]));
    let mut after_every_name = empty_name[..empty_name.len() - 1].to_vec();
    let kind = after_every_name
        .last_mut()
        .expect("an encoded key ends with its kind's tag before the empty name's terminator");
    *kind = kind
        .checked_add(1)
        .expect("a key kind's tag is below the largest byte");
    (
        Bound::Included(empty_name),
        Bound::Excluded(Bytes::from(after_every_name)),
    )
}

pub(crate) fn extended_attribute_name(key: &Bytes) -> Result<Bytes, CorruptValue> {
    match storekey::decode_borrow::<FsKey>(key) {
        Ok(FsKey {
            kind: KeyKind::ExtendedAttribute(Cow::Borrowed(name)),
            ..
        }) => Ok(key.slice_ref(name)),
        Ok(FsKey {
            kind: KeyKind::ExtendedAttribute(Cow::Owned(name)),
            ..
        }) => Ok(Bytes::from(name)),
        Ok(FsKey {
            kind: KeyKind::Attributes,
            ..
        }) => Err(CorruptValue::UnexpectedKey),
        Err(error) => Err(CorruptValue::UndecodableKey(error)),
    }
}

#[cfg(test)]
mod tests;
