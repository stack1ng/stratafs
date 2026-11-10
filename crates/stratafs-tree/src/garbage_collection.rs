// The garbage collector applies a volume's dereference jobs: it lowers each node's or blob's count, and deletes a node
// no parent points at any more, writing a job for each of its children, or for each blob a leaf's entries reference.
// A blob no leaf entry references any more is deleted from the content store by a later pass.

use std::convert::Infallible;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures_channel::oneshot;
use futures_util::future::{join_all, select, try_join_all};
use stratafs_runtime::Runtime;
use stratafs_store::content::ContentStore;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError,
};
use stratafs_store::store::Store;
use zerocopy::IntoBytes;
use zerocopy::little_endian::{U64, U128};

use crate::error::TreeFailure;
use crate::header::{JobId, NodeId, TreeContentId, TreeVolumeId};
use crate::key::{Referenced, TreeKey};
use crate::node::Node;
use crate::node_ref_count::stored_count;
use crate::serialized::CorruptValue;
use crate::tree::{MetadataFailure, MetadataRejection, TreeConfig, TreeShared};
use crate::volume::{TreeVolume, refused_write_as_failure, with_tree_failure};

impl<S, A, C> TreeVolume<S, A, C>
where
    S: Store,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // collect_garbage runs one garbage collection pass over the volume.
    pub async fn collect_garbage(
        &self,
    ) -> Result<(), TreeFailure<MetadataFailure<S>, MetadataRejection<S>>> {
        self.shared.collect_garbage(self.id).await
    }
}

impl<S, A, C> TreeShared<S, A, C>
where
    S: Store,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // collect_garbage_until runs a pass over volume's jobs after each jittered interval, until stopped resolves.
    // It holds the tree only during a pass, so dropping the tree's last handle closes its store at once, unless a pass
    // is running.
    pub(crate) async fn collect_garbage_until(
        tree: Weak<Self>,
        runtime: Arc<dyn Runtime>,
        volume: TreeVolumeId,
        stopped: oneshot::Receiver<Infallible>,
    ) {
        let garbage_collection = async {
            while let Some(interval) = tree
                .upgrade()
                .map(|tree| tree.garbage_collection_interval())
            {
                runtime.sleep(interval).await;
                let Some(tree) = tree.upgrade() else {
                    break;
                };
                if let Err(error) = tree.collect_garbage(volume).await {
                    tracing::warn!(%error, "a garbage collection pass failed, so its jobs wait for the next one");
                }
            }
        };
        select(pin!(garbage_collection), stopped).await;
    }

    // garbage_collection_interval is between half and one and a half GARBAGE_COLLECTION_INTERVAL, so garbage collectors spread out.
    fn garbage_collection_interval(&self) -> Duration {
        let nanos = (C::GARBAGE_COLLECTION_INTERVAL.as_nanos() as u64).max(1);
        C::GARBAGE_COLLECTION_INTERVAL / 2
            + Duration::from_nanos((self.id_allocator)() as u64 % nanos)
    }

    // collect_garbage deletes the blobs volume's earlier passes left unreferenced, then applies its jobs a batch at a
    // time, from a random node on, until none is left. A blob waits a pass before its delete, so a read planned before
    // the write that dereferenced it still loads it. A failed delete fails the pass after its jobs are applied.
    pub(crate) async fn collect_garbage(
        &self,
        volume: TreeVolumeId,
    ) -> Result<(), TreeFailure<MetadataFailure<S>, MetadataRejection<S>>> {
        let deleted = async {
            while self.delete_unreferenced_content(volume).await? {}
            Ok(())
        }
        .await;
        let start = NodeId(U128::new((self.id_allocator)()));
        while self.apply_jobs(volume, start).await? {}
        deleted
    }

    // apply_jobs applies up to GARBAGE_COLLECTION_BATCH of volume's jobs, from start on and then from the first, in one
    // transaction that deletes them, so each applies once. It says whether there were any.
    pub(crate) async fn apply_jobs(
        &self,
        volume: TreeVolumeId,
        start: NodeId,
    ) -> Result<bool, TreeFailure<MetadataFailure<S>, MetadataRejection<S>>> {
        let first = Bytes::from(TreeKey::Dereference(
            volume,
            Referenced::Node(NodeId(U128::new(0))),
            JobId(0),
        ));
        let from = Bytes::from(TreeKey::Dereference(
            volume,
            Referenced::Node(start),
            JobId(0),
        ));
        let last = Bytes::from(TreeKey::Dereference(
            volume,
            Referenced::Content(TreeContentId(u128::MAX)),
            JobId(u128::MAX),
        ));
        self.store
            .metadata()
            .transact(|mut transaction| {
                let (first, from, last) = (first.clone(), from.clone(), last.clone());
                Box::pin(async move {
                    let mut jobs: Vec<_> = transaction
                        .get_range(from.clone()..=last, C::GARBAGE_COLLECTION_BATCH)
                        .await?
                        .map(|(key, _)| key)
                        .collect();
                    if jobs.len() < C::GARBAGE_COLLECTION_BATCH {
                        let wrapped = transaction
                            .get_range(first..from, C::GARBAGE_COLLECTION_BATCH - jobs.len())
                            .await?;
                        jobs.extend(wrapped.map(|(key, _)| key));
                    }
                    if jobs.is_empty() {
                        return Ok((false, transaction));
                    }
                    // jobs are in key order, so each node's or blob's jobs are adjacent
                    let mut dereferenced: Vec<(Referenced, Bytes, u64)> = Vec::new();
                    for key in jobs {
                        let Ok(TreeKey::Dereference(_, referenced, _)) = storekey::decode(&key[..])
                        else {
                            return Err(TreeFailure::Corrupt(CorruptValue::InvalidKey));
                        };
                        match dereferenced.last_mut() {
                            Some((last, _, jobs)) if *last == referenced => *jobs += 1,
                            _ => dereferenced.push((
                                referenced,
                                TreeKey::RefCount(referenced).into(),
                                1,
                            )),
                        }
                        transaction.clear(key)?;
                    }
                    let stored = try_join_all(
                        dereferenced
                            .iter()
                            .map(|(_, key, _)| transaction.get(key.clone())),
                    )
                    .await?;
                    let mut unreferenced = Vec::new();
                    for ((referenced, key, jobs), stored) in dereferenced.into_iter().zip(stored) {
                        let count =
                            stored_count(stored.as_deref()).map_err(TreeFailure::Corrupt)?;
                        match (count.checked_sub(jobs), referenced) {
                            (None, _) => {
                                return Err(TreeFailure::Corrupt(CorruptValue::RefCountBelowZero));
                            }
                            (Some(0), Referenced::Node(node)) => {
                                transaction.clear(key)?;
                                unreferenced.push(Bytes::from(TreeKey::Node(node)));
                            }
                            // the blob is in another store, so a later pass deletes it
                            (Some(0), Referenced::Content(id)) => {
                                transaction.clear(key)?;
                                transaction
                                    .set(TreeKey::ContentDeletion(volume, id).into(), Bytes::new())
                                    .map_err(|error| {
                                        refused_write_as_failure(with_tree_failure(error))
                                    })?;
                            }
                            // a missing count is 1
                            (Some(1), _) => transaction.clear(key)?,
                            (Some(count), _) => {
                                transaction.set(key, U64::new(count).as_bytes()).map_err(
                                    |error| refused_write_as_failure(with_tree_failure(error)),
                                )?
                            }
                        }
                    }
                    let stored =
                        try_join_all(unreferenced.iter().map(|key| transaction.get(key.clone())))
                            .await?;
                    for (key, stored) in unreferenced.into_iter().zip(stored) {
                        let stored =
                            stored.ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
                        let (_, read) =
                            Node::<Bytes, C::Range>::read(stored).map_err(TreeFailure::Corrupt)?;
                        // each child lost a parent, and each blob a leaf entry, which a later batch applies
                        let mut dereference = |referenced| {
                            let job = JobId((self.id_allocator)());
                            transaction
                                .set(
                                    TreeKey::Dereference(volume, referenced, job).into(),
                                    Bytes::new(),
                                )
                                .map_err(|error| refused_write_as_failure(with_tree_failure(error)))
                        };
                        match read {
                            Node::Internal(entries) => {
                                for index in 0..entries.len() {
                                    let entry =
                                        entries.get(index).expect("the index is in the node");
                                    let child = entry.map_err(TreeFailure::Corrupt)?.child;
                                    dereference(Referenced::Node(child))?;
                                }
                            }
                            Node::Leaf(entries) => {
                                for index in 0..entries.len() {
                                    let entry =
                                        entries.get(index).expect("the index is in the node");
                                    let (entry_key, value) = entry
                                        .and_then(|entry| entry.key_and_value())
                                        .map_err(TreeFailure::Corrupt)?;
                                    if let Some(reference) = C::referenced_content(entry_key, value)
                                    {
                                        dereference(Referenced::Content(reference.id().into()))?;
                                    }
                                }
                            }
                        }
                        transaction.clear(key)?;
                    }
                    Ok((true, transaction))
                })
            })
            .await
            .map_err(transact_failure)
    }

    // delete_unreferenced_content deletes up to GARBAGE_COLLECTION_BATCH of volume's blobs waiting for deletion, then
    // clears the keys of those it deleted. It says whether there were any, or fails as the first failed delete did.
    async fn delete_unreferenced_content(
        &self,
        volume: TreeVolumeId,
    ) -> Result<bool, TreeFailure<MetadataFailure<S>, MetadataRejection<S>>> {
        let range = Bytes::from(TreeKey::ContentDeletion(volume, TreeContentId(0)))
            ..=Bytes::from(TreeKey::ContentDeletion(volume, TreeContentId(u128::MAX)));
        let deletions: Vec<Bytes> = self
            .store
            .metadata()
            .read_transact(|transaction| {
                let range = range.clone();
                Box::pin(async move {
                    let deletions = transaction
                        .get_range(range, C::GARBAGE_COLLECTION_BATCH)
                        .await?;
                    Ok::<_, TreeFailure<MetadataFailure<S>, MetadataRejection<S>>>(
                        deletions.map(|(key, _)| key).collect(),
                    )
                })
            })
            .await
            .map_err(transact_failure)?;
        if deletions.is_empty() {
            return Ok(false);
        }
        // the commits that stopped referencing the blobs reach the disk first, so a crash brings none of them back
        self.store
            .ensure_durable(DurableAgainst::PowerFailure)
            .await
            .map_err(|failure| TreeFailure::Durability(Box::new(failure)))?;
        let deleted = join_all(deletions.iter().map(|key| async move {
            let Ok(TreeKey::ContentDeletion(_, id)) = storekey::decode(&key[..]) else {
                return Err(TreeFailure::Corrupt(CorruptValue::InvalidKey));
            };
            self.store
                .content()
                .delete(&id.into())
                .await
                .map_err(|failure| TreeFailure::ContentDeletion(Box::new(failure)))
        }))
        .await;
        let (deletions, results) = (&deletions, &deleted);
        self.store
            .metadata()
            .transact(|mut transaction| {
                Box::pin(async move {
                    for (key, result) in deletions.iter().zip(results) {
                        if result.is_ok() {
                            transaction.clear(key.clone())?;
                        }
                    }
                    Ok::<_, TreeFailure<MetadataFailure<S>, MetadataRejection<S>>>((
                        (),
                        transaction,
                    ))
                })
            })
            .await
            .map_err(transact_failure)?;
        deleted.into_iter().collect::<Result<Vec<()>, _>>()?;
        Ok(true)
    }
}

// transact_failure is the tree failure a garbage collection transaction ends in.
fn transact_failure<F, R>(error: TransactError<R, F, TreeFailure<F, R>>) -> TreeFailure<F, R> {
    match error {
        TransactError::Rejected(rejection) => TreeFailure::RefusedTransaction(rejection),
        TransactError::Failed(failure) => TreeFailure::Store(failure),
        TransactError::Aborted(failure) => failure,
    }
}
