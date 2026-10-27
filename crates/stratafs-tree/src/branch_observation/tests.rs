use std::convert::Infallible;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::future::{block_on, poll_once};
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbFailure, RedbStore};
use stratafs_store::error::StoreError;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::kv::{KeyValueTransaction, SetRejection};

use super::*;
use crate::tree::Tree;

struct SmallNodeConfig;

impl TreeConfig for SmallNodeConfig {
    type Range = u8;
    const NODE_CAPACITY: usize = 128;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

fn open_volume() -> TreeVolume<RedbStore<Inline>, impl Fn() -> u128, SmallNodeConfig> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let next_id = AtomicU64::new(1);
    let allocate_id = move || u128::from(next_id.fetch_add(1, Ordering::Relaxed));
    block_on(async {
        let tree = Tree::new(RedbStore::new(database, Inline).unwrap(), allocate_id)
            .await
            .unwrap();
        let id = tree.create().await.unwrap();
        tree.open(id).await.unwrap()
    })
}

fn set_every_key<S, A, C>(volume: &TreeVolume<S, A, C>, value: &'static [u8])
where
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    block_on(volume.transact(|mut transaction| {
        Box::pin(async move {
            for index in 0..64 {
                transaction.set(
                    Bytes::from(format!("key/{index:02}")),
                    Bytes::from_static(value),
                )?;
            }
            Ok::<_, StoreError<SetRejection<S::Rejection>, TreeFailure<S::Failure, S::Rejection>>>(
                ((), transaction),
            )
        })
    }))
    .unwrap();
}

#[test]
fn waiters_on_the_same_key_share_its_watches() {
    let volume = open_volume();
    set_every_key(&volume, b"before");
    let key = Bytes::from_static(b"key/31");
    let seen = Some(Bytes::from_static(b"before"));
    let mut first = pin!(volume.changed(key.clone(), seen.clone()));
    let mut second = pin!(volume.changed(key, seen));
    assert!(block_on(poll_once(first.as_mut())).is_none());
    assert!(block_on(poll_once(second.as_mut())).is_none());
    {
        let watches = volume
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held_by: Vec<usize> = watches
            .values()
            .map(|watch| Arc::strong_count(&watch) - 1)
            .collect();
        assert!(
            held_by.len() > 2,
            "the key's branch is the volume header and more than one node"
        );
        assert!(
            held_by.iter().all(|&waiters| waiters == 2),
            "each watch is held by both waiters: {held_by:?}"
        );
    }
    set_every_key(&volume, b"after");
    assert!(block_on(first).is_ok());
    assert!(block_on(second).is_ok());
}

#[test]
fn a_watch_that_fired_is_not_handed_out_again_when_its_bytes_come_back() {
    let volume = open_volume();
    let set_watched = |value: &'static [u8]| {
        block_on(volume.shared.store.transact(|mut transaction| {
            Box::pin(async move {
                transaction.set(
                    Bytes::from_static(b"\xffwatched"),
                    Bytes::from_static(value),
                )?;
                Ok::<_, StoreError<SetRejection<Infallible>, RedbFailure>>(((), transaction))
            })
        }))
        .unwrap();
    };
    let (key, before) = (
        Bytes::from_static(b"\xffwatched"),
        Bytes::from_static(b"before"),
    );
    set_watched(b"before");
    let fired = volume.shared.watch(key.clone(), before.clone());
    set_watched(b"after");
    assert!(block_on(fired.changed.clone()).is_ok());
    set_watched(b"before");
    let again = volume.shared.watch(key, before);
    assert!(
        again.changed.peek().is_none(),
        "a waiter on bytes that came back gets a watch that has not fired"
    );
}
