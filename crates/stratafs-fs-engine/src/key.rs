use bytes::Bytes;
use storekey::Encode;

use crate::Inode;

#[derive(Encode)]
pub(crate) struct FsKey<'a> {
    // the inode goes here first to group the inode's keys together lexicographically
    pub(crate) inode: Inode,
    pub(crate) kind: KeyKind<'a>,
}

#[derive(Encode)]
pub(crate) enum KeyKind<'a> {
    Attributes,
    ExtendedAttribute(&'a [u8]),
}

impl From<FsKey<'_>> for Bytes {
    fn from(key: FsKey<'_>) -> Bytes {
        Bytes::from(
            storekey::encode_vec(&key)
                .expect("a derived encoding only fails on custom errors, and FsKey raises none"),
        )
    }
}

#[cfg(test)]
mod tests;
