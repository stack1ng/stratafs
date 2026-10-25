use std::iter;

use bytes::{Buf, Bytes};
use futures_util::future::select_all;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction, TransactError};

use crate::descent::DescentPath;
use crate::error::TreeFailure;
use crate::header::NodeId;
use crate::key::TreeKey;
use crate::node::{Node, NodeHeader, NodeKind};
use crate::tree::TreeConfig;
use crate::volume::{TreeTransaction, TreeVolume};

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
        let store = &self.shared.store;
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
            let (changed, _, _) = select_all(
                branch
                    .into_iter()
                    .map(|(key, value)| Box::pin(store.changed(key, Some(value)))),
            )
            .await;
            changed.map_err(TreeFailure::Store)?;
        }
    }
}

fn stored_node(id: NodeId, mut node: impl Buf) -> (Bytes, Bytes) {
    (
        TreeKey::Node(id).into(),
        node.copy_to_bytes(node.remaining()),
    )
}
