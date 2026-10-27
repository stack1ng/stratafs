use std::iter;
use std::sync::{Arc, PoisonError, Weak};

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::future::{Shared, select_all};
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction, TransactError};
use weak_table::WeakValueHashMap;

use crate::descent::DescentPath;
use crate::error::TreeFailure;
use crate::header::NodeId;
use crate::key::TreeKey;
use crate::node::{Node, NodeHeader, NodeKind};
use crate::tree::{TreeConfig, TreeShared};
use crate::volume::{TreeTransaction, TreeVolume};

pub(crate) type SharedWatches<F> =
    WeakValueHashMap<Bytes, Weak<SharedWatch<F>>, foldhash::fast::RandomState>;

// SharedWatch is one watch on a stored entry, shared by every waiter watching the same bytes,
// so whichever of them is polled drives it.
pub(crate) struct SharedWatch<F> {
    value: Bytes,
    changed: Shared<BoxFuture<'static, Result<(), Arc<F>>>>,
}

impl<S, A, C> TreeShared<S, A, C>
where
    S: KeyValueStore,
    A: Send + Sync + 'static,
    C: TreeConfig,
{
    // watch returns the watch on key for value, starting one unless a watch on those bytes is still waiting.
    // A new watch takes over the key, while the waiters of the one it replaces keep theirs.
    fn watch(self: &Arc<Self>, key: Bytes, value: Bytes) -> Arc<SharedWatch<S::Failure>> {
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(watch) = watches
            .get(&key)
            .filter(|watch| watch.value == value && watch.changed.peek().is_none())
        {
            return watch;
        }
        let (shared, watched_key, watched_value) = (self.clone(), key.clone(), value.clone());
        let watch = Arc::new(SharedWatch {
            value,
            changed: async move {
                shared
                    .store
                    .changed(watched_key, Some(watched_value))
                    .await
                    .map_err(Arc::new)
            }
            .boxed()
            .shared(),
        });
        watches.insert(key, watch.clone());
        watch
    }
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: ReadKeyValueTransaction,
    S: KeyValueStore,
    A: Send + Sync,
    C: TreeConfig,
{
    // branch returns the stored entries a change to the descended key has to change:
    // the volume header and every node on the key's path that belongs to the volume's current epoch.
    // Older nodes never change, so a change under one of them shows up in a current one above it.
    fn branch(&self, descent: &DescentPath<C::Range>) -> Vec<(Bytes, Bytes)> {
        let epoch = descent.volume.epoch;
        let internal_nodes = descent
            .steps
            .iter()
            .filter(|step| step.node.epoch == epoch)
            .map(|step| {
                let header = NodeHeader {
                    kind: NodeKind::Internal,
                    epoch,
                };
                stored_node(step.node.id, Node::write(&header, &step.entries))
            });
        let leaf = iter::once(&descent.destination)
            .filter(|leaf| leaf.node.epoch == epoch)
            .map(|leaf| {
                let header = NodeHeader {
                    kind: NodeKind::Leaf,
                    epoch,
                };
                stored_node(leaf.node.id, Node::write(&header, &leaf.entries))
            });
        iter::once((
            TreeKey::VolumeHeader(self.volume).into(),
            Bytes::from(&descent.volume),
        ))
        .chain(internal_nodes)
        .chain(leaf)
        .collect()
    }
}

impl<S, A, C> TreeVolume<S, A, C>
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // branch_changed resolves once the key's value differs from seen. Until then it waits for
    // any entry of the key's branch to change, then descends again.
    pub(crate) async fn branch_changed(
        &self,
        key: Bytes,
        seen: Option<Bytes>,
    ) -> Result<(), TreeFailure<S::Failure, S::Rejection>> {
        loop {
            let branch = self
                .read_transact(|transaction| {
                    let (key, seen) = (key.clone(), seen.clone());
                    Box::pin(async move {
                        let descent = transaction.descend(&key).await?;
                        let current = descent
                            .destination
                            .found_value()
                            .map_err(TreeFailure::Corrupt)?;
                        if current != seen.as_deref() {
                            return Ok(None);
                        }
                        Ok(Some(transaction.branch(&descent)))
                    })
                })
                .await
                .map_err(|error| match error {
                    TransactError::Rejected(rejection) => {
                        TreeFailure::RefusedTransaction(rejection)
                    }
                    TransactError::Failed(failure) | TransactError::Aborted(failure) => failure,
                })?;
            let Some(branch) = branch else {
                return Ok(());
            };
            let watches: Vec<_> = branch
                .into_iter()
                .map(|(key, value)| self.shared.watch(key, value))
                .collect();
            let (changed, _, _) =
                select_all(watches.iter().map(|watch| watch.changed.clone())).await;
            changed.map_err(TreeFailure::Watch)?;
        }
    }
}

fn stored_node(id: NodeId, mut node: impl Buf) -> (Bytes, Bytes) {
    (
        TreeKey::Node(id).into(),
        node.copy_to_bytes(node.remaining()),
    )
}

#[cfg(test)]
mod tests;
