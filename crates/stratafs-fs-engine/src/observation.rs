use bytes::Bytes;
use stratafs_store::kv::KeyValueStore;
use stratafs_store::store::Store;

use crate::attributes::{Attributes, StoredAttributes};
use crate::key::FsKey;
use crate::{FsEngine, Inode, MetadataFailure};

impl<St: Store, A, N, S> FsEngine<St, A, N, S> {
    // changed resolves once the inode differs from seen. Every change to an inode's content, attributes,
    // annotations or entries records itself in its attributes, and deleting it removes them.
    pub fn changed(
        &self,
        inode: Inode,
        seen: &Attributes,
    ) -> impl Future<Output = Result<(), MetadataFailure<St>>> + Send {
        self.store.metadata().changed(
            FsKey::new_attributes(inode).into(),
            Some(Bytes::from(&StoredAttributes::from(seen))),
        )
    }
}

#[cfg(test)]
mod tests;
