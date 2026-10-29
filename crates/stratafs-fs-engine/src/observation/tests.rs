use std::marker::PhantomData;
use std::pin::pin;
use std::sync::mpsc;
use std::time::Duration;

use futures_lite::future::{block_on, poll_once};

use super::*;
use crate::annotations::{AnnotationKey, SetAnnotationMode};
use crate::directories::NewItem;
use crate::test_support::{InMemoryEngine, ThreadSpawner, new_test_engine, new_test_engine_for};

// ChannelKernel is a kernel cache that sends each inode it is asked to invalidate.
struct ChannelKernel(mpsc::Sender<Inode>);

impl KernelCache for ChannelKernel {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, ()> {
        let _ = self.0.send(inode);
        Box::pin(async {})
    }
}

// auto_invalidating_engine is a test engine whose inodes a kernel caches, with the inodes it invalidates.
fn auto_invalidating_engine() -> (InMemoryEngine, mpsc::Receiver<Inode>) {
    let (sender, invalidated) = mpsc::channel();
    (new_test_engine_for(ChannelKernel(sender)), invalidated)
}

// another_client is a second engine on engine's store, whose inodes no kernel caches, so its writes are another
// client's.
fn another_client(engine: &InMemoryEngine) -> InMemoryEngine {
    FsEngine {
        allocate_inode: engine.allocate_inode,
        now: engine.now,
        store: Arc::clone(&engine.store),
        durability_limit: None,
        consistency: Arc::new(Eventual),
        auto_invalidators: Arc::new(AutoInvalidators::new(NoKernel, ThreadSpawner)),
        config: PhantomData,
    }
}

// NOTHING_YET is how long a test waits to see that nothing is invalidated.
const NOTHING_YET: Duration = Duration::from_millis(200);

fn new_file(engine: &InMemoryEngine, name: &'static [u8]) -> (Inode, Attributes) {
    let created = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.create(Inode::ROOT, name, NewItem::File, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    (created.inode, created.item)
}

fn write(engine: &InMemoryEngine, inode: Inode, data: &'static [u8]) {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(data)).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
}

#[test]
fn an_eventual_engine_invalidates_a_cached_inode_once_a_write_to_it_commits() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    let (other, _) = new_file(&engine, b"other");
    let mut invalidated = pin!(engine.invalidated(inode, &seen));
    assert!(block_on(poll_once(invalidated.as_mut())).is_none());
    write(&engine, other, b"elsewhere");
    assert!(block_on(poll_once(invalidated.as_mut())).is_none());
    write(&engine, inode, b"here");
    assert!(block_on(invalidated).is_ok());
}

#[test]
fn an_eventual_engine_invalidates_at_once_when_seen_is_already_stale() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    write(&engine, inode, b"before the watch");
    assert!(
        block_on(poll_once(engine.invalidated(inode, &seen)))
            .unwrap()
            .is_ok()
    );
}

#[test]
fn an_eventual_engine_invalidates_a_cached_inode_once_its_annotations_change() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    let invalidated = engine.invalidated(inode, &seen);
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn
                .set_annotation(
                    inode,
                    AnnotationKey::Portable(b"a"),
                    &b"1"[..],
                    SetAnnotationMode::Create,
                )
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert!(block_on(invalidated).is_ok());
}

#[test]
fn an_eventual_engine_invalidates_a_cached_inode_once_it_is_deleted() {
    let engine = new_test_engine();
    new_file(&engine, b"cached");
    let unlinked = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.hard_unlink(Inode::ROOT, b"cached", None).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    let (inode, seen) = (unlinked.inode, unlinked.item);
    let mut invalidated = pin!(engine.invalidated(inode, &seen));
    assert!(block_on(poll_once(invalidated.as_mut())).is_none());
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.delete_if_unlinked(inode).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert!(block_on(invalidated).is_ok());
}

#[test]
fn an_auto_invalidating_engine_invalidates_a_cached_inode_once_another_client_changes_it() {
    let (engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    write(&another_client(&engine), inode, b"elsewhere");
    assert_eq!(invalidated.recv_timeout(Duration::from_secs(10)), Ok(inode));
}

#[test]
fn an_auto_invalidating_engine_leaves_a_cached_inode_alone_when_it_writes_the_inode_itself() {
    let (engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    write(&engine, inode, b"here");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}

#[test]
fn an_auto_invalidating_engine_leaves_a_forgotten_inode_alone() {
    let (engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    engine.forget(inode);
    write(&another_client(&engine), inode, b"elsewhere");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}

#[test]
fn a_dropped_engine_leaves_what_it_cached_alone() {
    let (engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    let other = another_client(&engine);
    drop(engine);
    write(&other, inode, b"elsewhere");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}
