use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, NotCommitted, ReadKeyValueTransaction};

use crate::{FsEngine, FsTransaction, Inode};

impl<T: ReadKeyValueTransaction, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn observe(&self, inode: Inode) -> Result<T::Observation, T::Failure> {
        todo!()
    }
}

impl<M: KeyValueStore, C, A, N, S> FsEngine<M, C, A, N, S> {
    pub async fn changed(
        &self,
        observation: M::Observation,
    ) -> Result<(), StoreError<NotCommitted, M::Failure>> {
        self.metadata.changed(observation).await
    }
}
