use std::iter;

use bytes::Bytes;
use futures_util::future::{select_all, try_join_all};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, NotCommitted, ReadKeyValueTransaction, TransactError};

use crate::descent::DescentPath;
use crate::error::TreeFailure;
use crate::key::TreeKey;
use crate::tree::TreeConfig;
use crate::volume::{TreeTransaction, TreeVolume};

pub struct BranchWatch<O> {
    key: Bytes,
    initial_value: Option<Bytes>,
    // the volume header and every node on the key's path that belongs to the volume's current epoch.
    // Older nodes never change, so a change under one of them shows up in a current one above it.
    watches: Vec<O>,
}

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
        descent: &DescentPath<C::Range>,
    ) -> Result<BranchWatch<T::Observation>, TreeFailure<T::Failure, S::Rejection>> {
        // a write transaction's own buffered writes come before what the tree holds
        let initial_value = match self.buffered_delta.get(&key) {
            Some(buffered) => buffered.clone(),
            None => descent
                .destination
                .found_value()
                .map_err(TreeFailure::Corrupt)?
                .map(Bytes::copy_from_slice),
        };
        let current_nodes = descent
            .steps
            .iter()
            .map(|step| step.node)
            .chain(iter::once(descent.destination.node))
            .filter(|node| node.epoch == descent.volume.epoch)
            .map(|node| TreeKey::Node(node.id));
        let watches = try_join_all(
            iter::once(TreeKey::VolumeHeader(self.volume))
                .chain(current_nodes)
                .map(|key| self.inner.observe(key.into())),
        )
        .await?;
        Ok(BranchWatch {
            key,
            initial_value,
            watches,
        })
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
        mut watch: BranchWatch<S::Observation>,
    ) -> Result<(), StoreError<NotCommitted, TreeFailure<S::Failure, S::Rejection>>> {
        let store = &self.shared.store;
        loop {
            let (changed, _, _) = select_all(
                watch
                    .watches
                    .into_iter()
                    .map(|observation| Box::pin(store.changed(observation))),
            )
            .await;
            changed.map_err(|error| match error {
                StoreError::Rejected(not_committed) => StoreError::Rejected(not_committed),
                StoreError::Failed(failure) => StoreError::Failed(TreeFailure::Store(failure)),
            })?;

            let next_watch = self
                .read_transact(|transaction| {
                    let (key, initial_value) = (watch.key.clone(), watch.initial_value.clone());
                    Box::pin(async move {
                        let current_descent = transaction.descend(&key).await?;
                        let current_value = current_descent
                            .destination
                            .found_value()
                            .map_err(TreeFailure::Corrupt)?;
                        if current_value != initial_value.as_deref() {
                            return Ok(None);
                        }
                        Ok(Some(
                            transaction.observe_branch(key, &current_descent).await?,
                        ))
                    })
                })
                .await
                .map_err(|error| {
                    StoreError::Failed(match error {
                        TransactError::Rejected(rejection) => {
                            TreeFailure::RefusedTransaction(rejection)
                        }
                        TransactError::Failed(failure) | TransactError::Aborted(failure) => failure,
                    })
                })?;
            match next_watch {
                None => return Ok(()),
                Some(next_watch) => watch = next_watch,
            }
        }
    }
}
