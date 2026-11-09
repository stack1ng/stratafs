// The garbage collector applies a volume's dereference jobs: it lowers each node's count, and deletes a node no
// parent points at any more, writing a job for each of its children.

use std::convert::Infallible;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures_channel::oneshot;
use futures_util::future::{select, try_join_all};
use stratafs_runtime::Runtime;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError,
};
use zerocopy::IntoBytes;
use zerocopy::little_endian::{U64, U128};

use crate::error::TreeFailure;
use crate::header::{JobId, NodeId, TreeVolumeId};
use crate::key::TreeKey;
use crate::node::Node;
use crate::node_ref_count::stored_count;
use crate::serialized::CorruptValue;
use crate::tree::{TreeConfig, TreeShared};
use crate::volume::{refused_write_as_failure, with_tree_failure};

impl<S, A, C> TreeShared<S, A, C>
where
    S: KeyValueStore,
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

    // collect_garbage applies volume's jobs a batch at a time, from a random node on, until none is left.
    pub(crate) async fn collect_garbage(
        &self,
        volume: TreeVolumeId,
    ) -> Result<(), TreeFailure<S::Failure, S::Rejection>> {
        let start = NodeId(U128::new((self.id_allocator)()));
        while self.apply_jobs(volume, start).await? {}
        Ok(())
    }

    // apply_jobs applies up to GARBAGE_COLLECTION_BATCH of volume's jobs, from start on and then from the first, in one
    // transaction that deletes them, so each applies once. It says whether there were any.
    pub(crate) async fn apply_jobs(
        &self,
        volume: TreeVolumeId,
        start: NodeId,
    ) -> Result<bool, TreeFailure<S::Failure, S::Rejection>> {
        let first = Bytes::from(TreeKey::Dereference(volume, NodeId(U128::new(0)), JobId(0)));
        let from = Bytes::from(TreeKey::Dereference(volume, start, JobId(0)));
        let last = Bytes::from(TreeKey::Dereference(
            volume,
            NodeId(U128::new(u128::MAX)),
            JobId(u128::MAX),
        ));
        self.store
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
                    // jobs are in key order, so each node's jobs are adjacent
                    let mut dereferenced: Vec<(NodeId, Bytes, u64)> = Vec::new();
                    for key in jobs {
                        let Ok(TreeKey::Dereference(_, node, _)) = storekey::decode(&key[..])
                        else {
                            return Err(TreeFailure::Corrupt(CorruptValue::InvalidKey));
                        };
                        match dereferenced.last_mut() {
                            Some((last, _, jobs)) if *last == node => *jobs += 1,
                            _ => dereferenced.push((node, TreeKey::NodeRefCount(node).into(), 1)),
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
                    for ((node, key, jobs), stored) in dereferenced.into_iter().zip(stored) {
                        let count =
                            stored_count(stored.as_deref()).map_err(TreeFailure::Corrupt)?;
                        match count.checked_sub(jobs) {
                            None => {
                                return Err(TreeFailure::Corrupt(CorruptValue::RefCountBelowZero));
                            }
                            Some(0) => {
                                transaction.clear(key)?;
                                unreferenced.push(Bytes::from(TreeKey::Node(node)));
                            }
                            // a missing count is 1
                            Some(1) => transaction.clear(key)?,
                            Some(count) => {
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
                        // each child lost a parent, which a later batch applies
                        if let Node::Internal(entries) = read {
                            for index in 0..entries.len() {
                                let entry = entries.get(index).expect("the index is in the node");
                                let child = entry.map_err(TreeFailure::Corrupt)?.child;
                                let job = JobId((self.id_allocator)());
                                transaction
                                    .set(
                                        TreeKey::Dereference(volume, child, job).into(),
                                        Bytes::new(),
                                    )
                                    .map_err(|error| {
                                        refused_write_as_failure(with_tree_failure(error))
                                    })?;
                            }
                        }
                        transaction.clear(key)?;
                    }
                    Ok((true, transaction))
                })
            })
            .await
            .map_err(|error| match error {
                TransactError::Rejected(rejection) => TreeFailure::RefusedTransaction(rejection),
                TransactError::Failed(failure) => TreeFailure::Store(failure),
                TransactError::Aborted(failure) => failure,
            })
    }
}
