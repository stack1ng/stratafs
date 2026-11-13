use std::convert::Infallible;
use std::fmt::Debug;
use std::future::pending;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;

use bytes::Buf;
use futures_lite::future::{block_on, poll_once};
use futures_util::future::try_join_all;
use stratafs_pubsub::PubSubError;
use stratafs_pubsub_conformance::MemoryClient;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, TransactError};

use super::*;
use crate::annotations::{AnnotationKey, SetAnnotationMode};
use crate::attributes::Mode;
use crate::directories::NewItem;
use crate::error::EngineFailure;
use crate::holds::LATE_READS_WAIT;
use crate::kernel_cache::{Answered, Asked, NoKernel, Uninvalidated};
use crate::test_support::{
    InMemoryStore, OneKibInline, PUBLISH_TIMEOUT, TEST_MOUNT, TestEngine, TestVolumes,
    new_test_engine, new_test_engine_reaching, new_test_engine_with,
};
use crate::{
    Eventual, MetadataRejection, MountId, ResultHandling, StrongUntilDisconnectCached, Timestamp,
    WriteTransaction,
};

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

type StrongEngine = TestEngine<StrongUntilDisconnectCached, Alone>;

type Failed = Failure<Alone, InMemoryStore>;

// Settled is why a transaction announcing or ending a write in flight failed.
type Settled = TransactError<
    MetadataRejection<InMemoryStore>,
    MetadataFailure<InMemoryStore>,
    StoreWork<MetadataRejection<InMemoryStore>, MetadataFailure<InMemoryStore>>,
>;

fn strong_engine() -> StrongEngine {
    strong_engine_with(Arc::default())
}

// strong_engine_with is a test engine whose publishes time out while unanswered.
fn strong_engine_with(unanswered: Arc<AtomicBool>) -> StrongEngine {
    new_test_engine_reaching(
        move || {
            let unanswered = Arc::clone(&unanswered);
            Box::pin(async move { Ok(Alone { unanswered }) })
        },
        TIMEOUT,
        StrongUntilDisconnectCached,
        NoRoom(2),
    )
}

// new_file makes a file in the root.
fn new_file<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
) -> Inode {
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
fn new_file_another_mount_writes<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
) -> Inode {
    let inode = new_file(engine);
    let announced: Result<(), Settled> = block_on(
        engine
            .specification
            .mount
            .store
            .metadata()
            .transact(|mut metadata| {
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
            }),
    );
    announced.unwrap();
    inode
}

// end_other_mounts_write commits the write new_file_another_mount_writes left in flight on inode.
fn end_other_mounts_write<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
    inode: Inode,
) {
    let committed: Result<(), Settled> = block_on(
        engine
            .specification
            .mount
            .store
            .metadata()
            .transact(|mut metadata| {
                Box::pin(async move {
                    // The other mount signs what it commits, as every mount of a signed volume does.
                    let verifier = engine
                        .specification
                        .mount
                        .verifier
                        .as_ref()
                        .expect("signed");
                    let (mut attributes, root) = verifier
                        .read(&*metadata, inode)
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
                    let signed = verifier.signed(inode, &attributes, &root).await;
                    metadata
                        .set(FsKey::new_signature(inode).into(), signed.unwrap())
                        .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                    metadata
                        .clear(FsKey::new_pending_change_version(inode).into())
                        .map_err(|failure| StoreWork::Write(StoreError::Failed(failure)))?;
                    Ok(((), metadata))
                })
            }),
    );
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
    let state = &engine.state;
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
    assert!(matches!(written, Err(ConsistencyFailure::Contended)));
    // One that waits outlasts the other mount's write, and settles it once its writer is taken for gone.
    write(&engine, inode, PurgeWaits::OfOwnInode).expect("the write waits and commits");
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_on_another_mounts_write_while_no_delivery_is_handled()
 {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let strong = engine.consistency();
    let (stored, pending) = block_on(engine.mount().stored(&[inode])).unwrap();
    let in_flight = [(inode, stored[0].clone().expect("the file exists"))];
    let ticket = Cached::<InMemoryStore, Alone>::reading(strong, engine.mount());
    let mut settled = pin!(Cached::<InMemoryStore, Alone>::settled(
        strong,
        engine.mount(),
        &in_flight,
        &pending,
        ticket,
        PurgeWaits::OfAnyInode,
    ));
    assert!(block_on(poll_once(settled.as_mut())).is_none());
    engine.state.wake();
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
    assert!(matches!(served, Err(ConsistencyFailure::Contended)));
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_to_publish_while_another_publishes() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    let mount = engine.mount();
    let publishing = block_on(mount.publishing(PurgeWaits::OfAnyInode, &NoRoom(2)));
    let publishing = publishing
        .unwrap()
        .expect("one fewer than the limit may publish");
    let mut written = pin!(writing(&engine, inode, PurgeWaits::OfAnyInode));
    assert!(block_on(poll_once(written.as_mut())).is_none());
    // It waits having claimed the file, before it announces.
    assert!(mount.state.lock().claims.contains_key(&inode));
    let (_, pending) = block_on(mount.stored(&[inode])).unwrap();
    assert!(pending.is_empty());
    drop(publishing);
    block_on(written).expect("the write publishes and commits");
    assert_eq!(mount.state.lock().publishing, 0);
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waiting_to_publish_fails_once_a_delivery_is_handled() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    let mount = engine.mount();
    let publishing = block_on(mount.publishing(PurgeWaits::OfAnyInode, &NoRoom(2)));
    let _publishing = publishing
        .unwrap()
        .expect("one fewer than the limit may publish");
    let mut written = pin!(writing(&engine, inode, PurgeWaits::OfAnyInode));
    assert!(block_on(poll_once(written.as_mut())).is_none());
    start_delivery(&engine, Inode::ROOT);
    let written = block_on(poll_once(written.as_mut()));
    assert!(matches!(written, Some(Err(ConsistencyFailure::Contended))));
}

#[test]
fn with_a_limit_of_one_a_write_any_purge_waits_for_fails_rather_than_wait_to_publish() {
    let engine = strong_engine();
    let mount = engine.mount();
    let publishing = pin!(mount.publishing(PurgeWaits::OfAnyInode, &NoRoom(1)));
    let publishing = block_on(poll_once(publishing));
    assert!(matches!(
        publishing,
        Some(Err(ConsistencyFailure::Contended))
    ));
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
        Err(ConsistencyFailure::Publish(PubSubError::TimedOut))
    ));
    assert_eq!(engine.state.lock().publishing, 0);
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_fails_rather_than_wait_on_a_claim() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    // Another write of this mount, which never ends, claims the file.
    assert!(engine.mount().claim(u64::MAX, &[inode]));
    start_delivery(&engine, Inode::ROOT);
    let written = write(&engine, inode, PurgeWaits::OfAnyInode);
    assert!(matches!(written, Err(ConsistencyFailure::Contended)));
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waits_on_a_claim_while_no_delivery_is_handled() {
    let engine = strong_engine();
    let inodes = [new_file(&engine)];
    let mount = engine.mount();
    assert!(mount.claim(u64::MAX, &inodes));
    let mut unclaimed = pin!(mount.unclaimed(0, &inodes, PurgeWaits::OfAnyInode, &NoRoom(2)));
    assert!(block_on(poll_once(unclaimed.as_mut())).is_none());
    mount.unclaim(u64::MAX);
    block_on(unclaimed).expect("the write waits until the claim ends");
}

#[test]
fn with_no_room_a_write_any_purge_waits_for_waiting_on_a_claim_fails_once_a_delivery_is_handled() {
    let engine = strong_engine();
    let inodes = [new_file(&engine)];
    let mount = engine.mount();
    assert!(mount.claim(u64::MAX, &inodes));
    let mut unclaimed = pin!(mount.unclaimed(0, &inodes, PurgeWaits::OfAnyInode, &NoRoom(2)));
    assert!(block_on(poll_once(unclaimed.as_mut())).is_none());
    start_delivery(&engine, Inode::ROOT);
    let unclaimed = block_on(poll_once(unclaimed.as_mut()));
    assert!(matches!(
        unclaimed,
        Some(Err(ConsistencyFailure::Contended))
    ));
}

#[test]
fn a_read_waiting_on_another_mounts_write_wakes_at_its_commit() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let began = engine.specification.mount.runtime.now();
    let mut read = pin!(getattr(&engine, inode));
    assert!(block_on(poll_once(read.as_mut())).is_none());
    // It waits having met the write in flight.
    assert!(!engine.state.lock().seen.is_empty());
    end_other_mounts_write(&engine, inode);
    let read = block_on(read).expect("the read waits and goes on");
    assert_eq!(read.change_version.mount, OTHER_MOUNT);
    // Well before the other mount's writer would be taken for gone.
    assert!(engine.specification.mount.runtime.now() - began < TIMEOUT + SETTLE_MARGIN);
}

#[test]
fn a_read_waiting_on_a_write_whose_writer_is_gone_settles_it_at_its_deadline() {
    let engine = strong_engine();
    let inode = new_file_another_mount_writes(&engine);
    let began = engine.specification.mount.runtime.now();
    let read = block_on(getattr(&engine, inode)).expect("the read settles the write and goes on");
    assert!(engine.specification.mount.runtime.now() - began >= TIMEOUT + SETTLE_MARGIN);
    assert_eq!(read.change_version.mount, OTHER_MOUNT);
    let (_, pending) = block_on(engine.mount().stored(&[inode])).unwrap();
    assert!(pending.is_empty());
}

#[test]
fn a_late_read_settling_after_its_wait_is_over_goes_on() {
    let engine = strong_engine();
    let inode = new_file(&engine);
    block_on(getattr(&engine, inode)).expect("the read holds the inode");
    let strong = engine.consistency();
    // A delivery whose round asked the kernel longer ago than a late read waits for it, and has not ended.
    let asked_at = engine.specification.mount.runtime.now() - LATE_READS_WAIT;
    {
        let mut inner = engine.state.lock();
        let hold = inner.held(inode).expect("the mount holds the inode");
        hold.pending += 1;
        hold.asked += 1;
        hold.asked_at = Some(asked_at);
    }
    let (stored, pending) = block_on(engine.mount().stored(&[inode])).unwrap();
    let reads = [(inode, stored[0].clone().expect("the file exists"))];
    let ticket = Cached::<InMemoryStore, Alone>::reading(strong, engine.mount());
    let settled = Cached::<InMemoryStore, Alone>::settled(
        strong,
        engine.mount(),
        &reads,
        &pending,
        ticket,
        PurgeWaits::OfOwnInode,
    );
    let settled = block_on(
        engine
            .specification
            .mount
            .runtime
            .timeout(TIMEOUT + SETTLE_MARGIN, settled),
    );
    assert!(matches!(settled, Some(Ok(()))));
}

// ChannelKernel is a kernel cache that sends each inode it is asked to drop the copy of.
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

    fn invalidate_attributes(&self, _: Inode) -> Result<(), Uninvalidated> {
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

// write_data writes data at the start of inode.
fn write_data<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
    inode: Inode,
    data: &'static [u8],
) {
    block_on(engine.transact(PurgeWaits::OfOwnInode, |mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(data)).await;
            result.map(|_| ((), txn))
        })
    }))
    .unwrap();
}

// served serves inode's attributes, which the kernel caches when the result handling lets it.
fn served<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
    inode: Inode,
) -> (Attributes, ResultHandling) {
    let served = block_on(engine.serve(
        PurgeWaits::OfOwnInode,
        false,
        |txn| {
            Box::pin(async move {
                let found = txn.getattr(inode).await;
                found.map(|found| (found, txn))
            })
        },
        |found| found.iter().map(|found| (inode, found.clone())).collect(),
        |found, _, result_handling| {
            let handling = result_handling();
            found.clone().map(|found| (found, handling))
        },
    ));
    served.unwrap()
}

#[test]
fn an_eventual_mounts_write_returns_once_a_strong_mount_holding_the_file_dropped_its_copy() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let (sender, dropped) = mpsc::channel();
    let strong = volumes.engine_reaching(
        volume,
        MountId(1),
        volumes.connect(),
        PUBLISH_TIMEOUT,
        StrongUntilDisconnectCached,
        ChannelKernel(sender),
    );
    let eventual = volumes.engine_reaching(
        volume,
        MountId(2),
        volumes.connect(),
        PUBLISH_TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let inode = new_file(&eventual);
    let (_, handling) = served(&strong, inode);
    assert_eq!(handling, ResultHandling::Cache);
    while dropped.try_recv().is_ok() {}
    write_data(&eventual, inode, b"longer");
    assert_eq!(dropped.try_recv(), Ok(inode));
    let (attributes, _) = served(&strong, inode);
    assert_eq!(attributes.settable.size, 6);
    // The eventual mount holds nothing, so no write waits on it.
    assert!(eventual.state.lock().holds.is_empty());
}

#[test]
fn an_eventual_mounts_write_that_cannot_reach_the_pub_sub_fails_and_commits_nothing() {
    let engine = new_test_engine_reaching::<MemoryClient<u64>, _>(
        || async { Err("the pub/sub is unreachable".into()) }.boxed(),
        PUBLISH_TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await;
            result.map(|result| (result, txn))
        })
    }));
    assert!(matches!(
        created,
        Err(TransactError::Failed(EngineFailure::Consistency(
            ConsistencyFailure::Connect(_)
        )))
    ));
    let found = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.lookup(Inode::ROOT, b"file").await })),
    );
    assert!(found.unwrap().found.is_none());
    let (_, pending) = block_on(engine.mount().stored(&[Inode::ROOT])).unwrap();
    assert!(pending.is_empty());
}

// HungKernel is a kernel that never answers a request to drop the copy of an inode a delivery makes.
struct HungKernel;

impl KernelCache for HungKernel {
    fn invalidate(&self, _: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        Box::pin(async { Ok(()) })
    }

    fn invalidate_noting(
        &self,
        _: Inode,
        _: Asked,
        _: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        Box::pin(pending())
    }

    fn invalidate_attributes(&self, _: Inode) -> Result<(), Uninvalidated> {
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

#[test]
fn an_eventual_mounts_write_a_hung_strong_holder_never_answers_fails_and_commits_nothing() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let strong = volumes.engine_reaching(
        volume,
        MountId(1),
        volumes.connect(),
        PUBLISH_TIMEOUT,
        StrongUntilDisconnectCached,
        HungKernel,
    );
    let eventual = volumes.engine_reaching(
        volume,
        MountId(2),
        volumes.connect(),
        TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let inode = new_file(&eventual);
    served(&strong, inode);
    let written = block_on(eventual.transact(PurgeWaits::OfOwnInode, |mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(b"lost")).await;
            result.map(|_| ((), txn))
        })
    }));
    assert!(matches!(
        written,
        Err(TransactError::Failed(EngineFailure::Consistency(
            ConsistencyFailure::Publish(_)
        )))
    ));
    let (stored, pending) = block_on(eventual.mount().stored(&[inode])).unwrap();
    assert_eq!(
        stored[0].as_ref().expect("the file exists").settable.size,
        0
    );
    assert!(pending.is_empty());
}

#[test]
fn an_eventual_mount_that_cannot_reach_the_pub_sub_still_serves_reads() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let reachable = volumes.engine_with(volume, Eventual::default(), NoKernel);
    let inode = new_file(&reachable);
    write_data(&reachable, inode, b"kept");
    let unreachable = volumes.engine_reaching::<MemoryClient<u64>, _>(
        volume,
        MountId(2),
        || async { Err("the pub/sub is unreachable".into()) }.boxed(),
        PUBLISH_TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let (seen, handling) = served(&unreachable, inode);
    assert_eq!((seen.settable.size, handling), (4, ResultHandling::Cache));
    let found = block_on(unreachable.serve(
        PurgeWaits::No,
        false,
        |txn| {
            Box::pin(async move {
                let lookup = txn.lookup(Inode::ROOT, b"file").await;
                lookup.map(|lookup| (lookup, txn))
            })
        },
        |lookup| lookup.found.iter().cloned().collect(),
        |lookup, _, _| Some(lookup.found.as_ref().map(|(found, _)| *found)),
    ));
    assert_eq!(found.unwrap(), Some(inode));
    let written = block_on(unreachable.transact(PurgeWaits::OfOwnInode, |mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(b"lost")).await;
            result.map(|_| ((), txn))
        })
    }));
    assert!(matches!(
        written,
        Err(TransactError::Failed(EngineFailure::Consistency(
            ConsistencyFailure::Connect(_)
        )))
    ));
    assert_eq!(content(&reachable, inode), b"kept");
    assert_eq!(attributes(&reachable, inode), seen);
}

#[test]
fn an_eventual_mounts_write_settles_a_write_whose_writer_is_gone_at_its_deadline() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let eventual = volumes.engine_reaching(
        volume,
        TEST_MOUNT,
        volumes.connect(),
        TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let inode = new_file_another_mount_writes(&eventual);
    let began = eventual.specification.mount.runtime.now();
    write_data(&eventual, inode, b"after");
    assert!(eventual.specification.mount.runtime.now() - began >= TIMEOUT + SETTLE_MARGIN);
    let (stored, pending) = block_on(eventual.mount().stored(&[inode])).unwrap();
    let stored = stored[0].clone().expect("the file exists");
    // The settle moved the version to the other mount's announcement, and the write moved it once more.
    assert_eq!(stored.change_version.number, 2);
    assert_eq!(stored.settable.size, 5);
    assert!(pending.is_empty());
}

// ATTEMPTS is the most attempts a write of one transaction takes, so a write that runs again and again fails.
const ATTEMPTS: usize = 4;

type Transaction<'txn, 'engine> =
    WriteTransaction<'txn, 'engine, InMemoryStore, fn() -> u64, fn() -> Timestamp, OneKibInline>;

// commits runs work as one write on engine, which must commit within ATTEMPTS attempts, and is its result.
fn commits<'engine, C: Cached<InMemoryStore, MemoryClient<u64>>, T: Send + 'engine, E>(
    engine: &'engine TestEngine<C>,
    mut work: impl for<'txn> FnMut(
        Transaction<'txn, 'engine>,
    ) -> BoxFuture<'txn, Result<(T, Transaction<'txn, 'engine>), E>>
    + Send,
) -> T
where
    E: Send + Debug + 'engine,
{
    let attempts = AtomicUsize::new(0);
    let committed = block_on(engine.transact(PurgeWaits::No, |txn| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        assert!(attempt < ATTEMPTS, "the write ran {attempt} times");
        work(txn)
    }));
    committed.unwrap()
}

// attributes is inode's stored attributes.
fn attributes<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
    inode: Inode,
) -> Attributes {
    let (stored, _) = block_on(engine.mount().stored(&[inode])).unwrap();
    stored[0].clone().expect("the inode exists")
}

// content is inode's stored content.
fn content<C: Cached<InMemoryStore, P>, P: PubSub<Message = u64>>(
    engine: &TestEngine<C, P>,
    inode: Inode,
) -> Vec<u8> {
    let plan = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.read(inode, 0, 4096).await })),
    );
    let pieces = block_on(try_join_all(plan.unwrap().load(engine.content()))).unwrap();
    pieces
        .into_iter()
        .flat_map(|mut piece| piece.copy_to_bytes(piece.remaining()))
        .collect()
}

// annotation is the value of inode's portable annotation named name, if it has one.
fn annotation<C: Cached<InMemoryStore, MemoryClient<u64>>>(
    engine: &TestEngine<C>,
    inode: Inode,
    name: &'static [u8],
) -> Option<Bytes> {
    let value = block_on(engine.read_transact(|txn| {
        Box::pin(async move {
            let value = txn
                .get_annotation(inode, AnnotationKey::Portable(name))
                .await;
            value.map(|value| value.map(|mut value| value.copy_to_bytes(value.remaining())))
        })
    }));
    value.unwrap()
}

// named is the inode name refers to in directory, if any.
fn named<C: Cached<InMemoryStore, MemoryClient<u64>>>(
    engine: &TestEngine<C>,
    directory: Inode,
    name: &'static [u8],
) -> Option<Inode> {
    let lookup = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.lookup(directory, name).await })),
    );
    lookup.unwrap().found.map(|(inode, _)| inode)
}

// on_both runs shape on an eventual engine and on a strong one.
macro_rules! on_both {
    ($shape:ident) => {
        $shape(&new_test_engine());
        $shape(&new_test_engine_with(StrongUntilDisconnectCached, NoKernel));
    };
}

fn writes_twice<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = new_file(engine);
    let before = attributes(engine, inode).change_version.number;
    commits(engine, |mut txn| {
        Box::pin(async move {
            txn.write(inode, 0, Bytes::from_static(b"a")).await.unwrap();
            let result = txn.write(inode, 1, Bytes::from_static(b"b")).await;
            result.map(|_| ((), txn))
        })
    });
    assert_eq!(attributes(engine, inode).change_version.number, before + 1);
    assert_eq!(content(engine, inode), b"ab");
}

#[test]
fn a_transaction_that_writes_a_file_twice_commits() {
    on_both!(writes_twice);
}

fn sets_and_writes<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = new_file(engine);
    let before = attributes(engine, inode).change_version.number;
    commits(engine, |mut txn| {
        Box::pin(async move {
            txn.setattr(inode, |settable| settable.mode = Mode::OWNER_READ)
                .await
                .unwrap();
            let result = txn.write(inode, 0, Bytes::from_static(b"a")).await;
            result.map(|_| ((), txn))
        })
    });
    let after = attributes(engine, inode);
    assert_eq!(after.change_version.number, before + 1);
    assert_eq!(after.settable.mode, Mode::OWNER_READ);
    assert_eq!(content(engine, inode), b"a");
}

#[test]
fn a_transaction_that_sets_attributes_and_writes_a_file_commits() {
    on_both!(sets_and_writes);
}

fn creates_twice<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let before = attributes(engine, Inode::ROOT).change_version.number;
    let (a, b) = commits(engine, |mut txn| {
        Box::pin(async move {
            let a = txn
                .create(Inode::ROOT, b"a", NewItem::File, |_| {})
                .await
                .unwrap();
            let result = txn.create(Inode::ROOT, b"b", NewItem::File, |_| {}).await;
            result.map(|b| ((a.inode, b.inode), txn))
        })
    });
    assert_eq!(
        attributes(engine, Inode::ROOT).change_version.number,
        before + 1
    );
    assert_eq!(named(engine, Inode::ROOT, b"a"), Some(a));
    assert_eq!(named(engine, Inode::ROOT, b"b"), Some(b));
}

#[test]
fn a_transaction_that_creates_two_files_in_one_directory_commits() {
    on_both!(creates_twice);
}

fn annotates_and_sets<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = new_file(engine);
    let before = attributes(engine, inode).change_version.number;
    commits(engine, |mut txn| {
        Box::pin(async move {
            txn.set_annotation(
                inode,
                AnnotationKey::Portable(b"a"),
                &b"1"[..],
                SetAnnotationMode::Create,
            )
            .await
            .unwrap();
            let result = txn
                .setattr(inode, |settable| settable.mode = Mode::OWNER_READ)
                .await;
            result.map(|_| ((), txn))
        })
    });
    let after = attributes(engine, inode);
    assert_eq!(after.change_version.number, before + 1);
    assert_eq!(after.settable.mode, Mode::OWNER_READ);
    assert_eq!(annotation(engine, inode, b"a").as_deref(), Some(&b"1"[..]));
}

#[test]
fn a_transaction_that_annotates_a_file_and_sets_its_attributes_commits() {
    on_both!(annotates_and_sets);
}

fn creates_and_writes<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = commits(engine, |mut txn| {
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            let result = txn.write(created.inode, 0, Bytes::from_static(b"a")).await;
            result.map(|_| (created.inode, txn))
        })
    });
    assert_eq!(attributes(engine, inode).change_version.number, 0);
    assert_eq!(named(engine, Inode::ROOT, b"file"), Some(inode));
    assert_eq!(content(engine, inode), b"a");
}

#[test]
fn a_transaction_that_creates_and_writes_a_file_commits() {
    on_both!(creates_and_writes);
}

fn creates_and_sets<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = commits(engine, |mut txn| {
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            let result = txn
                .setattr(created.inode, |settable| settable.mode = Mode::OWNER_READ)
                .await;
            result.map(|_| (created.inode, txn))
        })
    });
    let after = attributes(engine, inode);
    assert_eq!(after.change_version.number, 0);
    assert_eq!(after.settable.mode, Mode::OWNER_READ);
}

#[test]
fn a_transaction_that_creates_a_file_and_sets_its_attributes_commits() {
    on_both!(creates_and_sets);
}

fn creates_and_annotates<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = commits(engine, |mut txn| {
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            let result = txn
                .set_annotation(
                    created.inode,
                    AnnotationKey::Portable(b"a"),
                    &b"1"[..],
                    SetAnnotationMode::Create,
                )
                .await;
            result.map(|_| (created.inode, txn))
        })
    });
    assert_eq!(attributes(engine, inode).change_version.number, 0);
    assert_eq!(annotation(engine, inode, b"a").as_deref(), Some(&b"1"[..]));
}

#[test]
fn a_transaction_that_creates_and_annotates_a_file_commits() {
    on_both!(creates_and_annotates);
}

fn creates_and_links<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = commits(engine, |mut txn| {
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            let result = txn.hard_link(created.inode, Inode::ROOT, b"link").await;
            result.map(|_| (created.inode, txn))
        })
    });
    let after = attributes(engine, inode);
    assert_eq!(after.change_version.number, 0);
    assert_eq!(after.hard_link_count, 2);
    assert_eq!(named(engine, Inode::ROOT, b"file"), Some(inode));
    assert_eq!(named(engine, Inode::ROOT, b"link"), Some(inode));
}

#[test]
fn a_transaction_that_creates_and_links_a_file_commits() {
    on_both!(creates_and_links);
}

fn creates_in_a_new_directory<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let (directory, file) = commits(engine, |mut txn| {
        Box::pin(async move {
            let made = txn
                .create(Inode::ROOT, b"d", NewItem::Directory, |_| {})
                .await
                .unwrap();
            let result = txn.create(made.inode, b"f", NewItem::File, |_| {}).await;
            result.map(|file| ((made.inode, file.inode), txn))
        })
    });
    assert_eq!(attributes(engine, directory).change_version.number, 0);
    assert_eq!(named(engine, Inode::ROOT, b"d"), Some(directory));
    assert_eq!(named(engine, directory, b"f"), Some(file));
}

#[test]
fn a_transaction_that_creates_a_file_in_a_directory_it_made_commits() {
    on_both!(creates_in_a_new_directory);
}

fn unlinks_and_deletes<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = new_file(engine);
    commits(engine, |mut txn| {
        Box::pin(async move {
            txn.hard_unlink(Inode::ROOT, b"file", None).await.unwrap();
            let result = txn.delete_if_unlinked(inode).await;
            result.map(|_| ((), txn))
        })
    });
    let (stored, pending) = block_on(engine.mount().stored(&[inode])).unwrap();
    assert!(stored[0].is_none());
    assert!(pending.is_empty());
    assert_eq!(named(engine, Inode::ROOT, b"file"), None);
}

#[test]
fn a_transaction_that_unlinks_and_deletes_a_file_commits() {
    on_both!(unlinks_and_deletes);
}

fn creates_and_deletes<C: Cached<InMemoryStore, MemoryClient<u64>>>(engine: &TestEngine<C>) {
    let inode = commits(engine, |mut txn| {
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            txn.hard_unlink(Inode::ROOT, b"file", None).await.unwrap();
            let result = txn.delete_if_unlinked(created.inode).await;
            result.map(|_| (created.inode, txn))
        })
    });
    let (stored, _) = block_on(engine.mount().stored(&[inode])).unwrap();
    assert!(stored[0].is_none());
    assert_eq!(named(engine, Inode::ROOT, b"file"), None);
}

#[test]
fn a_transaction_that_creates_and_deletes_a_file_commits() {
    on_both!(creates_and_deletes);
}

#[test]
fn a_strong_mount_lets_go_of_a_file_its_committing_attempt_made_and_deleted() {
    let engine = new_test_engine_with(StrongUntilDisconnectCached, NoKernel);
    let attempts = AtomicUsize::new(0);
    let inode = commits(&engine, |mut txn| {
        let deletes = attempts.fetch_add(1, Ordering::SeqCst) > 0;
        Box::pin(async move {
            let created = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap();
            if deletes {
                txn.hard_unlink(Inode::ROOT, b"file", None).await.unwrap();
                txn.delete_if_unlinked(created.inode).await.unwrap();
            }
            Ok::<_, Infallible>((created.inode, txn))
        })
    });
    assert!(attempts.into_inner() > 1, "the first attempt made the file");
    assert!(!engine.state.lock().holds.contains_key(&inode));
}

#[test]
fn an_eventual_mounts_reply_overtaken_after_a_forget_hands_over_what_it_committed() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let eventual = volumes.engine_with(volume, Eventual::default(), NoKernel);
    let other = volumes.engine_reaching(
        volume,
        MountId(2),
        volumes.connect(),
        PUBLISH_TIMEOUT,
        Eventual::default(),
        NoKernel,
    );
    let inode = new_file(&eventual);
    let mut overtaken = false;
    let handed = block_on(eventual.transact_and_serve(
        PurgeWaits::OfOwnInode,
        |mut txn| {
            Box::pin(async move {
                let result = txn
                    .setattr(inode, |settable| settable.mode = Mode::OWNER_READ)
                    .await;
                result.map(|attributes| (attributes, txn))
            })
        },
        |attributes| vec![(inode, attributes.clone())],
        |_, reads, result_handling| {
            // The kernel lets go of the file as the reply would go, and another mount changes it.
            if !overtaken {
                overtaken = true;
                eventual.forget(inode);
                write_data(&other, inode, b"other");
            }
            // As FSKit's, the reply goes only cached.
            (result_handling() == ResultHandling::Cache).then(|| reads[0].1.clone())
        },
    ));
    let handed = handed.expect("a write that committed replies");
    assert!(overtaken);
    assert_eq!(handed.settable.mode, Mode::OWNER_READ);
    assert_eq!(handed.settable.size, 0);
}
