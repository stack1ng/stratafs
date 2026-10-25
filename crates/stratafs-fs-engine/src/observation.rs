use stratafs_store::kv::KeyValueStore;

use crate::attributes::Attributes;
use crate::{FsEngine, Inode};

impl<M: KeyValueStore, C, A, N, S> FsEngine<M, C, A, N, S> {
    pub async fn changed(&self, inode: Inode, seen: &Attributes) -> Result<(), M::Failure> {
        todo!()
    }
}
