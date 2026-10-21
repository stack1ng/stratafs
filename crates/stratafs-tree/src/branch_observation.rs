use bytes::Bytes;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, NotCommitted, ReadKeyValueTransaction};

use crate::error::TreeFailure;
use crate::tree::TreeConfig;
use crate::volume::{TreeTransaction, TreeVolume};

pub struct BranchWatch;

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    A: Send + Sync,
    C: TreeConfig,
{
    pub(crate) async fn observe_branch(
        &self,
        key: Bytes,
    ) -> Result<BranchWatch, TreeFailure<T::Failure, S::Rejection>> {
        todo!()
    }
}

impl<S, A, C> TreeVolume<S, A, C>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(crate) async fn branch_changed(
        &self,
        watch: BranchWatch,
    ) -> Result<(), StoreError<NotCommitted, TreeFailure<S::Failure, S::Rejection>>> {
        todo!()
    }
}
