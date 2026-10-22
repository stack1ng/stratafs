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
    DirectoryEntry(u64),
    HardLink { dir: Inode, position: u64 },
    SymbolicLinkTarget,
}

const INODE_AND_KIND_LENGTH: usize = size_of::<u64>() + 1;

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

    pub(crate) fn new_directory_entry(dir: Inode, position: u64) -> Self {
        FsKey {
            inode: dir,
            kind: KeyKind::DirectoryEntry(position),
        }
    }

    pub(crate) fn new_hard_link(inode: Inode, dir: Inode, position: u64) -> Self {
        FsKey {
            inode,
            kind: KeyKind::HardLink { dir, position },
        }
    }

    pub(crate) fn new_symbolic_link_target(inode: Inode) -> Self {
        FsKey {
            inode,
            kind: KeyKind::SymbolicLinkTarget,
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

fn after_every_key_of_kind(key: &[u8]) -> Bytes {
    let mut after = key[..INODE_AND_KIND_LENGTH].to_vec();
    let kind = after
        .last_mut()
        .expect("an encoded key starts with its inode and then its kind's tag");
    *kind = kind
        .checked_add(1)
        .expect("a key kind's tag is below the largest byte");
    Bytes::from(after)
}

pub(crate) fn extended_attributes_range(inode: Inode) -> (Bound<Bytes>, Bound<Bytes>) {
    let empty_name = Bytes::from(FsKey::new_extended_attribute(inode, &[]));
    let after_every_name = after_every_key_of_kind(&empty_name);
    (
        Bound::Included(empty_name),
        Bound::Excluded(after_every_name),
    )
}

pub(crate) fn directory_entries_after(dir: Inode, after: u64) -> (Bound<Bytes>, Bound<Bytes>) {
    let after = Bytes::from(FsKey::new_directory_entry(dir, after));
    let after_every_entry = after_every_key_of_kind(&after);
    (Bound::Excluded(after), Bound::Excluded(after_every_entry))
}

pub(crate) fn directory_entries_between(
    dir: Inode,
    first: u64,
    last: u64,
) -> (Bound<Bytes>, Bound<Bytes>) {
    (
        Bound::Included(FsKey::new_directory_entry(dir, first).into()),
        Bound::Included(FsKey::new_directory_entry(dir, last).into()),
    )
}

pub(crate) fn hard_links_of(inode: Inode) -> (Bound<Bytes>, Bound<Bytes>) {
    let first = Bytes::from(FsKey::new_hard_link(inode, Inode(0), 0));
    let after_every_entry = after_every_key_of_kind(&first);
    (Bound::Included(first), Bound::Excluded(after_every_entry))
}

pub(crate) fn every_key_of(inode: Inode) -> (Bound<Bytes>, Bound<Bytes>) {
    let next = Inode(
        inode
            .0
            .checked_add(1)
            .expect("inode numbers stop below the largest value"),
    );
    (
        Bound::Included(FsKey::new_attributes(inode).into()),
        Bound::Excluded(FsKey::new_attributes(next).into()),
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
        Ok(_) => Err(CorruptValue::UnexpectedKey),
        Err(error) => Err(CorruptValue::UndecodableKey(error)),
    }
}

pub(crate) fn directory_entry_position(key: &[u8]) -> Result<u64, CorruptValue> {
    match storekey::decode_borrow::<FsKey>(key) {
        Ok(FsKey {
            kind: KeyKind::DirectoryEntry(position),
            ..
        }) => Ok(position),
        Ok(_) => Err(CorruptValue::UnexpectedKey),
        Err(error) => Err(CorruptValue::UndecodableKey(error)),
    }
}

pub(crate) fn hard_link_directory(key: &[u8]) -> Result<Inode, CorruptValue> {
    match storekey::decode_borrow::<FsKey>(key) {
        Ok(FsKey {
            kind: KeyKind::HardLink { dir, .. },
            ..
        }) => Ok(dir),
        Ok(_) => Err(CorruptValue::UnexpectedKey),
        Err(error) => Err(CorruptValue::UndecodableKey(error)),
    }
}

#[cfg(test)]
mod tests;
