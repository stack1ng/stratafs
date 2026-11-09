use std::fmt::Debug;
use std::future::pending;
use std::pin::pin;
use std::sync::atomic::AtomicBool;

use futures_lite::future::{block_on, poll_once};

use super::*;
use crate::directories::NewItem;
use crate::error::EngineFailure;
use crate::test_support::{InMemoryStore, TestEngine, new_test_engine_with};

// TIMEOUT is how long a publish waits, short so a write waiting on a writer that is gone settles its write soon.
const TIMEOUT: Duration = Duration::from_millis(10);

// OTHER_MOUNT is a mount of the volume that announces a write and never ends it.
const OTHER_MOUNT: MountId = MountId(2);

// Alone is a volume's pub/sub no other mount holds anything of, so a publish reaches no one. While unanswered, a
// publish times out instead, as if a holder never handled it.
struct Alone {
    unanswered: Arc<AtomicBool>,
}

impl PubSub for Alone {
    type Message = u64;
    type Subscription = ();
    type Failure = Infallible;

    async fn subscribe<Handled>(
        &self,
        _: Key,
        _: impl Fn(u64) -> Handled + Send + Sync + 'static,
    ) -> Result<(), PubSubError<Infallible>>
    where
        Handled: Future<Output = ()> + Send + 'static,
    {
        Ok(())
    }

    async fn publish(&self, _: Key, _: &u64, _: Duration) -> Result<(), PubSubError<Infallible>> {
        match self.unanswered.load(Ordering::SeqCst) {
            true => Err(PubSubError::TimedOut),
            false => Ok(()),
        }
    }

    async fn failed(&self) -> Infallible {
        pending().await
    }
}

// NoRoom is a kernel with no room for another of the requests it keeps a limited number of in flight, as many as it
// holds.
struct NoRoom(usize);

impl KernelCache for NoRoom {
    fn invalidate(&self, _: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        Box::pin(async { Ok(()) })
    }

    fn invalidate_noting(
        &self,
        _: Inode,
        asked: Asked,
        answered: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        asked();
        answered(true);
        Box::pin(async { Ok(()) })
    }

    fn invalidate_attributes(&self, _: Inode) -> Result<(), Uninvalidated> {
        Ok(())
    }

    fn purge_waits_on_others(&self) -> bool {
        false
    }

    fn in_flight_room(&self) -> bool {
        false
    }

    fn in_flight_limit(&self) -> Option<usize> {
        Some(self.0)
    }
}

type StrongEngine = TestEngine<StrongUntilDisconnectCached<Alone>>;

type Failed = Failure<Alone, InMemoryStore>;

fn strong_engine() -> StrongEngine {
    strong_engine_with(Arc::default())
}

// strong_engine_with is a test engine whose publishes time out while unanswered.
fn strong_engine_with(unanswered: Arc<AtomicBool>) -> StrongEngine {
    let strong = StrongUntilDisconnectCached::new(
        move || {
            let unanswered = Arc::clone(&unanswered);
            Box::pin(async move { Ok(Alone { unanswered }) })
        },
        TIMEOUT,
    );
    new_test_engine_with(strong, NoRoom(2))
}

// new_file makes a file in the root.
fn new_file(engine: &StrongEngine) -> Inode {
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await;
            result.map(|result| (result, txn))
        })
    }));
    created.unwrap().inode
}

// new_file_another_mount_writes makes a file in the root, with another mount's write to it in flight.
fn new_file_another_mount_writes(engine: &StrongEngine) -> Inode {
    let inode = new_file(engine);
    let announced: Result<(), Settled<InMemoryStore>> =
        block_on(engine.mount.store.metadata().transact(|mut metadata| {
            Box::pin(async move {
                let attributes = read_attributes(&*metadata, inode)
                    .await
                    .map_err(StoreWork::Read)?
                    .expect("the file exists");
                metadata
                    .set(
                        FsKey::new_pending_change_version(inode).into(),
                        Bytes::from(&attributes.change_version.after(OTHER_MOUNT)),
                    )
                    .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                Ok(((), metadata))
            })
        }));
    announced.unwrap();
    inode
}

// end_other_mounts_write commits the write new_file_another_mount_writes left in flight on inode.
fn end_other_mounts_write(engine: &StrongEngine, inode: Inode) {
    let committed: Result<(), Settled<InMemoryStore>> =
        block_on(engine.mount.store.metadata().transact(|mut metadata| {
            Box::pin(async move {
                let mut attributes = read_attributes(&*metadata, inode)
                    .await
                    .map_err(StoreWork::Read)?
                    .expect("the file exists");
                attributes.change_version = attributes.change_version.after(OTHER_MOUNT);
                metadata
                    .set(
                        FsKey::new_attributes(inode).into(),
                        Bytes::from(&attributes),
                    )
                    .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                metadata
                    .clear(FsKey::new_pending_change_version(inode).into())
                    .map_err(|failure| StoreWork::Write(StoreError::Failed(failure)))?;
                Ok(((), metadata))
            })
        }));
    committed.unwrap();
}

// getattr reads inode's attributes as an operation no purge waits for, which waits on any write in flight it shows.
async fn getattr(engine: &StrongEngine, inode: Inode) -> Result<Attributes, Failed> {
    let served = engine.serve(
        PurgeWaits::No,
        false,
        |txn| {
            Box::pin(async move {
                let found = txn.getattr(inode).await;
                found.map(|found| (found, txn))
            })
        },
        |found| found.iter().map(|found| (inode, found.clone())).collect(),
        |found, _, result_handling| {
            (result_handling() == ResultHandling::Cache).then(|| found.clone())
        },
    );
    let found = served.await.map_err(consistency_failure)?;
    Ok(found.expect("the file exists"))
}

// start_delivery has the mount handle a delivery of inode, which it holds, that never ends.
fn start_delivery(engine: &StrongEngine, inode: Inode) {
    let state = &engine.consistency().state;
    state
        .lock()
        .held(inode)
        .expect("the mount holds the inode")
        .pending += 1;
    state.wake();
}

// write writes to inode as an operation of purge_waits.
fn write(engine: &StrongEngine, inode: Inode, purge_waits: PurgeWaits) -> Result<(), Failed> {
    block_on(writing(engine, inode, purge_waits))
}

// writing is write, to run as a future.
async fn writing(
    engine: &StrongEngine,
    inode: Inode,
    purge_waits: PurgeWaits,
) -> Result<(), Failed> {
    let written = engine.transact(purge_waits, |mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(b"x")).await;
            result.map(|_| ((), txn))
        })
    });
    written.await.map_err(consistency_failure)
}

// consistency_failure is why a transaction failed, which must be the consistency's.
fn consistency_failure<R: Debug, F: Debug, E: Debug>(
    failed: TransactError<R, EngineFailure<F, Failed>, E>,
) -> Failed {
    match failed {
        TransactError::Failed(EngineFailure::Consistency(failure)) => failure,
        other => panic!("the transaction failed outside the consistency: {other:?}"),
    }
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_fails_rather_than_wait_on_another_mounts_write() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    start_delivery(&engine, Inode::ROOT);
    let written = write(&engine, inode, PurgeWaits::OfAnyInode);
    assert!(matches!(written, Err(StrongFailure::Contended)));
    // One that waits outlasts the other mount's write, and settles it once its writer is taken for gone.
    write(&engine, inode, PurgeWaits::OfOwnInode).expect("the write waits and commits");
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_on_another_mounts_write_while_no_delivery_is_handled()
 {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let strong = engine.consistency();
    let (stored, pending) = block_on(strong.stored(&engine.mount, &[inode])).unwrap();
    let in_flight = [(inode, stored[0].clone().expect("the file exists"))];
    let ticket = Cached::<InMemoryStore>::reading(strong);
    let mut settled = pin!(Cached::<InMemoryStore>::settled(
        strong,
        &engine.mount,
        &in_flight,
        &pending,
        ticket,
        PurgeWaits::OfAnyInode,
    ));
    assert!(block_on(poll_once(settled.as_mut())).is_none());
    strong.state.wake();
    block_on(settled).expect("the write waits");
}

#[test]
fn with_no_room_a_read_any_purge_waits_for_fails_rather_than_wait_to_retry() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    start_delivery(&engine, Inode::ROOT);
    // As a read whose data the kernel keeps, a result it may not cache is taken again.
    let served = block_on(engine.serve(
        PurgeWaits::OfAnyInode,
        false,
        |txn| {
            Box::pin(async move {
                let found = txn.getattr(inode).await;
                found.map(|found| (found, txn))
            })
        },
        |found| found.iter().map(|found| (inode, found.clone())).collect(),
        |_, _, result_handling| (result_handling() == ResultHandling::Cache).then_some(()),
    ));
    let served = served.map_err(consistency_failure);
    assert!(matches!(served, Err(StrongFailure::Contended)));
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_to_publish_while_another_publishes() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    let strong = engine.consistency();
    let publishing =
        block_on(strong.publishing::<InMemoryStore>(PurgeWaits::OfAnyInode, &NoRoom(2)));
    let publishing = publishing
        .unwrap()
        .expect("one fewer than the limit may publish");
    let mut written = pin!(writing(&engine, inode, PurgeWaits::OfAnyInode));
    assert!(block_on(poll_once(written.as_mut())).is_none());
    // It waits having claimed the file, before it announces.
    assert!(strong.state.lock().claims.contains_key(&inode));
    let (_, pending) = block_on(strong.stored(&engine.mount, &[inode])).unwrap();
    assert!(pending.is_empty());
    drop(publishing);
    block_on(written).expect("the write publishes and commits");
    assert_eq!(strong.state.lock().publishing, 0);
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waiting_to_publish_fails_once_a_delivery_is_handled() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    let strong = engine.consistency();
    let publishing =
        block_on(strong.publishing::<InMemoryStore>(PurgeWaits::OfAnyInode, &NoRoom(2)));
    let _publishing = publishing
        .unwrap()
        .expect("one fewer than the limit may publish");
    let mut written = pin!(writing(&engine, inode, PurgeWaits::OfAnyInode));
    assert!(block_on(poll_once(written.as_mut())).is_none());
    start_delivery(&engine, Inode::ROOT);
    let written = block_on(poll_once(written.as_mut()));
    assert!(matches!(written, Some(Err(StrongFailure::Contended))));
}

#[test]
fn with_a_limit_of_one_a_write_any_purge_waits_for_fails_rather_than_wait_to_publish() {
    let engine = strong_engine();
    let strong = engine.consistency();
    let publishing = pin!(strong.publishing::<InMemoryStore>(PurgeWaits::OfAnyInode, &NoRoom(1)));
    let publishing = block_on(poll_once(publishing));
    assert!(matches!(publishing, Some(Err(StrongFailure::Contended))));
}

#[test]
fn a_write_any_purge_waits_for_whose_publish_fails_stops_counting_as_publishing() {
    let unanswered = Arc::new(AtomicBool::new(false));
    let engine = strong_engine_with(Arc::clone(&unanswered));
    let inode = new_file(&engine);
    unanswered.store(true, Ordering::SeqCst);
    let written = write(&engine, inode, PurgeWaits::OfAnyInode);
    assert!(matches!(
        written,
        Err(StrongFailure::Publish(PubSubError::TimedOut))
    ));
    assert_eq!(engine.consistency().state.lock().publishing, 0);
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_fails_rather_than_wait_on_a_claim() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    // Another write of this mount, which never ends, claims the file.
    assert!(engine.consistency().claim(u64::MAX, &[inode]));
    start_delivery(&engine, Inode::ROOT);
    let written = write(&engine, inode, PurgeWaits::OfAnyInode);
    assert!(matches!(written, Err(StrongFailure::Contended)));
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_on_a_claim_while_no_delivery_is_handled() {
    let engine = strong_engine();
    let inodes = [new_file(&engine)];
    let strong = engine.consistency();
    assert!(strong.claim(u64::MAX, &inodes));
    let mut unclaimed =
        pin!(strong.unclaimed::<InMemoryStore>(0, &inodes, PurgeWaits::OfAnyInode, &NoRoom(2)));
    assert!(block_on(poll_once(unclaimed.as_mut())).is_none());
    strong.unclaim(u64::MAX);
    block_on(unclaimed).expect("the write waits until the claim ends");
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waiting_on_a_claim_fails_once_a_delivery_is_handled() {
    let engine = strong_engine();
    let inodes = [new_file(&engine)];
    let strong = engine.consistency();
    assert!(strong.claim(u64::MAX, &inodes));
    let mut unclaimed =
        pin!(strong.unclaimed::<InMemoryStore>(0, &inodes, PurgeWaits::OfAnyInode, &NoRoom(2)));
    assert!(block_on(poll_once(unclaimed.as_mut())).is_none());
    start_delivery(&engine, Inode::ROOT);
    let unclaimed = block_on(poll_once(unclaimed.as_mut()));
    assert!(matches!(unclaimed, Some(Err(StrongFailure::Contended))));
}

#[test]
fn a_read_waiting_on_another_mounts_write_wakes_at_its_commit() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let began = engine.mount.runtime.now();
    let mut read = pin!(getattr(&engine, inode));
    assert!(block_on(poll_once(read.as_mut())).is_none());
    // It waits having met the write in flight.
    assert!(!engine.consistency().state.lock().seen.is_empty());
    end_other_mounts_write(&engine, inode);
    let read = block_on(read).expect("the read waits and goes on");
    assert_eq!(read.change_version.mount, OTHER_MOUNT);
    // Well before the other mount's writer would be taken for gone.
    assert!(engine.mount.runtime.now() - began < TIMEOUT + SETTLE_MARGIN);
}

#[test]
fn a_read_waiting_on_a_write_whose_writer_is_gone_settles_it_at_its_deadline() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let began = engine.mount.runtime.now();
    let read = block_on(getattr(&engine, inode)).expect("the read settles the write and goes on");
    assert!(engine.mount.runtime.now() - began >= TIMEOUT + SETTLE_MARGIN);
    assert_eq!(read.change_version.mount, OTHER_MOUNT);
    let (_, pending) = block_on(engine.consistency().stored(&engine.mount, &[inode])).unwrap();
    assert!(pending.is_empty());
}

#[test]
fn a_late_read_settling_after_its_wait_is_over_goes_on() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    block_on(getattr(&engine, inode)).expect("the read holds the inode");
    let strong = engine.consistency();
    // A delivery whose round asked the kernel longer ago than a late read waits for it, and has not ended.
    let asked_at = engine.mount.runtime.now() - LATE_READS_WAIT;
    {
        let mut inner = strong.state.lock();
        let hold = inner.held(inode).expect("the mount holds the inode");
        hold.pending += 1;
        hold.asked += 1;
        hold.asked_at = Some(asked_at);
    }
    let (stored, pending) = block_on(strong.stored(&engine.mount, &[inode])).unwrap();
    let reads = [(inode, stored[0].clone().expect("the file exists"))];
    let ticket = Cached::<InMemoryStore>::reading(strong);
    let settled = Cached::<InMemoryStore>::settled(
        strong,
        &engine.mount,
        &reads,
        &pending,
        ticket,
        PurgeWaits::OfOwnInode,
    );
    let settled = block_on(
        engine
            .mount
            .runtime
            .timeout(TIMEOUT + SETTLE_MARGIN, settled),
    );
    assert!(matches!(settled, Some(Ok(()))));
}
