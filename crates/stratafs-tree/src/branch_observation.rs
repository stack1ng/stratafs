use std::pin::pin;

use bytes::Bytes;
use futures_util::future::{Either, OptionFuture, join, pending, select};
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
    header_watch: O,
    // if the deepest node is the header, then this is none
    deepest_node_watch: Option<O>,
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
        let initial_value = descent
            .destination
            .found_value()
            .map_err(TreeFailure::Corrupt)?
            .map(Bytes::copy_from_slice);
        let deepest_same_epoch_node = if descent.destination.node.epoch == descent.volume.epoch {
            Some(descent.destination.node)
        } else {
            descent
                .steps
                .iter()
                .rfind(|step| step.node.epoch == descent.volume.epoch)
                .map(|step| step.node)
        };
        let (header_watch, deepest_node_watch) = join(
            self.inner
                .observe(TreeKey::VolumeHeader(self.volume).into()),
            OptionFuture::from(
                deepest_same_epoch_node
                    .map(|node| self.inner.observe(TreeKey::Node(node.id).into())),
            ),
        )
        .await;
        Ok(BranchWatch {
            key,
            initial_value,
            header_watch: header_watch?,
            deepest_node_watch: deepest_node_watch.transpose()?,
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
            let deepest_node_changed = match watch.deepest_node_watch {
                Some(deepest_node_watch) => Either::Left(store.changed(deepest_node_watch)),
                None => Either::Right(pending()),
            };
            match select(
                pin!(store.changed(watch.header_watch)),
                pin!(deepest_node_changed),
            )
            .await
            {
                Either::Left((changed, _)) | Either::Right((changed, _)) => {
                    changed.map_err(|error| match error {
                        StoreError::Rejected(not_committed) => StoreError::Rejected(not_committed),
                        StoreError::Failed(failure) => {
                            StoreError::Failed(TreeFailure::Store(failure))
                        }
                    })?
                }
            }

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
