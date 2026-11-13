use std::marker::PhantomData;
use std::pin::pin;
use std::sync::mpsc;
use std::time::Duration;

use futures_core::future::BoxFuture;
use futures_lite::future::{block_on, poll_once};
use stratafs_pubsub_conformance::MemoryClient;

use super::*;
use crate::annotations::{AnnotationKey, SetAnnotationMode};
use crate::checked_content::CheckedBlobs;
use crate::directories::NewItem;
use crate::kernel_cache::{Answered, Asked, KernelCache, NoKernel, Uninvalidated};
use crate::test_support::{
    InMemoryEngine, InMemoryStore, OneKibInline, PUBLISH_TIMEOUT, TestVolumes, new_test_engine,
};
use crate::{FsConfig, FsEngine, MountId, PurgeWaits, ResultHandling};

// ChannelKernel is a kernel cache that sends each inode it is asked to invalidate.
struct ChannelKernel(mpsc::Sender<Inode>);

impl KernelCache for ChannelKernel {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        let _ = self.0.send(inode);
        Box::pin(async { Ok(()) })
    }

    fn invalidate_noting(
        &self,
        inode: Inode,
        asked: Asked,
        answered: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        asked();
        let _ = self.0.send(inode);
        answered(true);
        Box::pin(async { Ok(()) })
    }

    fn invalidate_attributes(&self, _inode: Inode) -> Result<(), Uninvalidated> {
        Ok(())
    }

    fn purge_waits_on_others(&self) -> bool {
        false
    }

    fn in_flight_room(&self) -> bool {
        true
    }

    fn in_flight_limit(&self) -> Option<usize> {
        None
    }
}

// auto_invalidating_engine is a test engine of volumes whose inodes a kernel caches, with the inodes it invalidates.
fn auto_invalidating_engine() -> (TestVolumes, InMemoryEngine, mpsc::Receiver<Inode>) {
    let (sender, invalidated) = mpsc::channel();
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let engine = volumes.engine_with(volume, Eventual::default(), ChannelKernel(sender));
    (volumes, engine, invalidated)
}

// another_client is a second engine on engine's store, whose inodes no kernel caches, so its writes are another
// client's. It reaches the pub/sub of volumes, engine's.
fn another_client(volumes: &TestVolumes, engine: &InMemoryEngine) -> InMemoryEngine {
    FsEngine {
        allocate_inode: engine.allocate_inode,
        allocate_content_id: engine.allocate_content_id,
        now: engine.now,
        mount: Mount::new(
            Arc::clone(&engine.mount.store),
            Arc::new(NoKernel),
            Arc::clone(&engine.mount.runtime),
            MountId(2),
            engine.mount.verifier.clone(),
            volumes.connect(),
            PUBLISH_TIMEOUT,
        ),
        durability_limit: None,
        consistency: Eventual::default(),
        checked: CheckedBlobs::new(OneKibInline::CHECKED_CONTENT_BYTES),
        config: PhantomData,
    }
}

// NOTHING_YET is how long a test waits to see that nothing is invalidated.
const NOTHING_YET: Duration = Duration::from_millis(200);

fn new_file(engine: &InMemoryEngine, name: &'static [u8]) -> (Inode, Attributes) {
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn.create(Inode::ROOT, name, NewItem::File, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    (created.inode, created.item)
}

fn write(engine: &InMemoryEngine, inode: Inode, data: &'static [u8]) {
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
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
    let mut invalidated = pin!(invalidated(&*engine.mount.store, inode, &seen));
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
        block_on(poll_once(invalidated(&*engine.mount.store, inode, &seen)))
            .unwrap()
            .is_ok()
    );
}

#[test]
fn an_eventual_engine_invalidates_a_cached_inode_once_its_annotations_change() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    let invalidated = invalidated(&*engine.mount.store, inode, &seen);
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
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
    let unlinked = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn.hard_unlink(Inode::ROOT, b"cached", None).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    let (inode, seen) = (unlinked.inode, unlinked.item);
    let mut invalidated = pin!(invalidated(&*engine.mount.store, inode, &seen));
    assert!(block_on(poll_once(invalidated.as_mut())).is_none());
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
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
    let (volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    write(&another_client(&volumes, &engine), inode, b"elsewhere");
    assert_eq!(invalidated.recv_timeout(Duration::from_secs(10)), Ok(inode));
}

#[test]
fn an_auto_invalidating_engine_leaves_a_cached_inode_alone_when_it_writes_the_inode_itself() {
    let (_volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    write(&engine, inode, b"here");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}

#[test]
fn an_auto_invalidating_engine_leaves_a_forgotten_inode_alone() {
    let (volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    engine.forget(inode);
    write(&another_client(&volumes, &engine), inode, b"elsewhere");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}

#[test]
fn an_eventual_engine_retries_a_read_begun_before_the_kernel_last_let_go_of_an_inode() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    let reading =
        || Cached::<InMemoryStore, MemoryClient<u64>>::reading(&engine.consistency, &engine.mount);
    let reads = [(inode, seen)];
    let handling = |ticket| {
        engine.consistency.result_handling(
            &engine.mount,
            &reads,
            &Pending::new(),
            ticket,
            ticket,
            PurgeWaits::OfOwnInode,
            false,
        )
    };
    let before = reading();
    engine.forget(inode);
    assert_eq!(handling(before), ResultHandling::Retry);
    assert_eq!(handling(reading()), ResultHandling::Cache);
}

#[test]
fn a_dropped_engine_leaves_what_it_cached_alone() {
    let (volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    let other = another_client(&volumes, &engine);
    drop(engine);
    write(&other, inode, b"elsewhere");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
}

#[test]
fn an_auto_invalidating_engine_keeps_watching_after_its_own_write_and_invalidates_on_another_clients()
 {
    let (volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, seen) = new_file(&engine, b"cached");
    engine.cached(inode, &seen);
    write(&engine, inode, b"here");
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
    write(&another_client(&volumes, &engine), inode, b"elsewhere");
    assert_eq!(invalidated.recv_timeout(Duration::from_secs(10)), Ok(inode));
}

#[test]
fn an_older_read_cached_after_a_newer_one_is_invalidated_for_a_change_between_them() {
    let (volumes, engine, invalidated) = auto_invalidating_engine();
    let (inode, older) = new_file(&engine, b"cached");
    write(&another_client(&volumes, &engine), inode, b"elsewhere");
    let newer =
        block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
            .unwrap()
            .unwrap();
    engine.cached(inode, &newer);
    assert!(invalidated.recv_timeout(NOTHING_YET).is_err());
    engine.cached(inode, &older);
    assert_eq!(invalidated.recv_timeout(Duration::from_secs(10)), Ok(inode));
}
