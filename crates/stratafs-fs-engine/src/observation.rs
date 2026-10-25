use bytes::Bytes;
use stratafs_store::kv::KeyValueStore;

use crate::attributes::{Attributes, StoredAttributes};
use crate::key::FsKey;
use crate::{FsEngine, Inode};

impl<M: KeyValueStore, C, A, N, S> FsEngine<M, C, A, N, S> {
    // changed resolves once the inode differs from seen. Every change to an inode's content, attributes,
    // extended attributes or entries records itself in its attributes, and deleting it removes them.
    pub fn changed(
        &self,
        inode: Inode,
        seen: &Attributes,
    ) -> impl Future<Output = Result<(), M::Failure>> + Send {
        self.metadata.changed(
            FsKey::new_attributes(inode).into(),
            Some(Bytes::from(&StoredAttributes::from(seen))),
        )
    }
}

#[cfg(test)]
mod tests;
