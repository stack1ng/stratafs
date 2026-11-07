// The strong until disconnect, cached consistency. A write announces the version it will move each inode it changes to
// as the inode's pending change version, publishes it to the inode's holders, and commits once each dropped its copy.
// A mount holds every inode the kernel caches, hands the kernel nothing older than it acknowledged, and waits on writes
// in flight it may have missed. A write announces only while no other is in flight on the inode, so a number names it.

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use event_listener::Event;
use futures_core::future::BoxFuture;
use futures_util::future::{AbortHandle, Abortable, join_all, select};
use stratafs_pubsub::{Key, PubSub, PubSubError};
use stratafs_runtime::Runtime;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError,
};
use stratafs_store::store::Store;

use crate::attributes::{Attributes, ChangeVersion, read_attributes};
use crate::error::{IoFailure, PosixError, refused_or_failed};
use crate::kernel_cache::{Answered, Asked, KernelCache, Uninvalidated};
use crate::key::FsKey;
use crate::{
    Cached, Change, Consistency, Inode, MetadataFailure, MetadataRejection, Mount, MountId, Purge,
    ResultHandling,
};

// SETTLE_MARGIN is how much longer than a publish's timeout a mount waits on another mount's write in flight before
// it takes the writer for gone and settles the write itself.
const SETTLE_MARGIN: Duration = Duration::from_secs(1);

// CONNECT_TIMEOUT is how long connecting to the volume's pub/sub may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

// PURGE_RETRY is how soon a purge tries again to drop a copy the kernel would not, doubling up to PURGE_RETRY_MOST.
const PURGE_RETRY: Duration = Duration::from_millis(10);
const PURGE_RETRY_MOST: Duration = Duration::from_secs(1);

// LATE_READS_WAIT is how long after a round asks the kernel a read begun since waits for the round to end instead of
// making it invalidate again.
const LATE_READS_WAIT: Duration = Duration::from_millis(3);

// RECHECK is how often a read waiting on a write in flight it may have missed reads again.
const RECHECK: Duration = Duration::from_millis(5);

// StrongUntilDisconnectCached serves reads from a cache, which forces any write to verify that all other
// StrongUntilDisconnectCached clients have been invalidated before it can return. A client that disconnects
// may miss writes until it notices and fails.
pub struct StrongUntilDisconnectCached<P: PubSub> {
    // connect connects a client of the volume's pub/sub, first and again after the last one failed.
    connect: Box<dyn Fn() -> BoxFuture<'static, Result<P, ConnectFailure>> + Send + Sync>,
    // timeout is how long a publish waits for every other holder to handle it.
    timeout: Duration,
    state: Arc<State<P>>,
}

// ConnectFailure is why connecting to the volume's pub/sub failed.
pub type ConnectFailure = Box<dyn Error + Send + Sync>;

impl<P: PubSub> Consistency for StrongUntilDisconnectCached<P> {}

impl<P: PubSub<Message = u64>> StrongUntilDisconnectCached<P> {
    // new is the consistency for a volume whose pub/sub connect reaches and whose publishes wait timeout for holders.
    pub fn new(
        connect: impl Fn() -> BoxFuture<'static, Result<P, ConnectFailure>> + Send + Sync + 'static,
        timeout: Duration,
    ) -> Self {
        StrongUntilDisconnectCached {
            connect: Box::new(connect),
            timeout,
            state: Arc::new(State {
                inner: Mutex::new(Inner {
                    connection: Connection::Down {
                        connecting: false,
                        purging: false,
                    },
                    holds: HashMap::new(),
                    seen: HashMap::new(),
                    claims: HashMap::new(),
                    delivering: HashMap::new(),
                    generations: 0,
                    connects: 0,
                    unconnected: None,
                }),
                watching: Mutex::new(None),
                changed: Event::new(),
                clock: AtomicU64::new(1),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StrongFailure<F, R, M> {
    #[error("connecting to the volume's pub/sub failed")]
    Connect(#[source] Arc<dyn Error + Send + Sync>),
    #[error("connecting to the volume's pub/sub took longer than its time limit")]
    ConnectTimedOut,
    #[error("holding an inode failed")]
    Hold(#[source] PubSubError<F>),
    #[error("publishing a write failed")]
    Publish(#[source] PubSubError<F>),
    #[error("announcing or settling a write in the store failed")]
    Store(#[source] TransactError<R, M, StoreWork<R, M>>),
    #[error("reading or clearing pending change versions in the engine's transaction failed")]
    Pending(#[source] IoFailure<M, Infallible>),
    #[error("another write changed what a write committed before its reply could hand it over")]
    Overtaken,
    #[error("the mount is dropping the kernel's cache after its pub/sub connection failed")]
    Purging,
    #[error("dropping the kernel's copy of the attributes a write changes failed")]
    Uninvalidated(#[source] Uninvalidated),
    #[error(
        "another write is changing an inode this write changes, and cannot end before this mount drops a copy, which \
         the kernel may hold back while this write is in flight"
    )]
    Contended,
}

// A write another write in flight holds back is busy; every other failure is an I/O error.
impl<F, R, M> From<&StrongFailure<F, R, M>> for PosixError {
    fn from(failure: &StrongFailure<F, R, M>) -> Self {
        match failure {
            StrongFailure::Contended => PosixError::EBUSY,
            StrongFailure::Connect(_)
            | StrongFailure::ConnectTimedOut
            | StrongFailure::Hold(_)
            | StrongFailure::Publish(_)
            | StrongFailure::Store(_)
            | StrongFailure::Pending(_)
            | StrongFailure::Overtaken
            | StrongFailure::Purging
            | StrongFailure::Uninvalidated(_) => PosixError::EIO,
        }
    }
}

// StoreWork is why announcing or settling a write failed in its transaction: reading an inode's attributes or pending
// change version, or writing one.
#[derive(Debug, thiserror::Error)]
pub enum StoreWork<R, M> {
    #[error(transparent)]
    Read(IoFailure<M, Infallible>),
    #[error(transparent)]
    Write(StoreError<R, M>),
}

type Failure<P, St> =
    StrongFailure<<P as PubSub>::Failure, MetadataRejection<St>, MetadataFailure<St>>;
// Pending is the pending change version of each inode that has one.
pub type Pending = HashMap<Inode, ChangeVersion>;
type Settled<St> = TransactError<
    MetadataRejection<St>,
    MetadataFailure<St>,
    StoreWork<MetadataRejection<St>, MetadataFailure<St>>,
>;

// Prepared is what a write readied: the version it announced and published for each inode it changes, and the holds
// of the inodes its reply hands the kernel, which wait for the reply.
#[derive(Clone)]
pub struct Prepared(Arc<Readied>);

struct Readied {
    // write names the write among this mount's writes, in claims.
    write: u64,
    announced: HashMap<Inode, ChangeVersion>,
    // new is each inode the write makes, held before it exists, and deleted each it deletes.
    new: Vec<Inode>,
    deleted: Vec<Inode>,
    // replying is each hold the write's reply holds up, by inode and hold.
    replying: Vec<(Inode, u64)>,
}

struct State<P: PubSub> {
    inner: Mutex<Inner<P>>,
    // watching stops the task watching the connected client for failure.
    watching: Mutex<Option<AbortHandle>>,
    // changed wakes reads waiting on a hold, a delivery or a connection.
    changed: Event,
    // clock numbers holds as they start and reads as they begin, so a read can tell a hold made after it began.
    clock: AtomicU64,
}

struct Inner<P: PubSub> {
    connection: Connection<P>,
    holds: HashMap<Inode, Holding<P>>,
    // seen is when this mount first waited on each write in flight by another mount, by inode and announcement.
    seen: HashMap<(Inode, MountId, u64), Instant>,
    // claims is the write of this mount announcing each inode, until it finishes.
    claims: HashMap<Inode, u64>,
    // delivering is each write of this mount whose own delivery runs, and whether a write its purge may wait on asked
    // it to yield.
    delivering: HashMap<u64, bool>,
    // generations counts the clients connected so far.
    generations: u64,
    // connects counts the connects begun so far, and unconnected is the last that failed, by its count.
    connects: u64,
    unconnected: Option<(u64, Unconnected)>,
}

// Unconnected is why a connect failed, which every operation that waited on it fails with too.
#[derive(Clone)]
enum Unconnected {
    Failed(Arc<dyn Error + Send + Sync>),
    TimedOut,
}

impl<F, R, M> From<Unconnected> for StrongFailure<F, R, M> {
    fn from(unconnected: Unconnected) -> Self {
        match unconnected {
            Unconnected::Failed(failure) => StrongFailure::Connect(failure),
            Unconnected::TimedOut => StrongFailure::ConnectTimedOut,
        }
    }
}

enum Connection<P> {
    // Down has no client, and one may be connecting. A mount purging is dropping the kernel's copies after its
    // connection failed, and connects again only once it is done.
    Down { connecting: bool, purging: bool },
    Up { client: Arc<P>, generation: u64 },
}

enum Holding<P: PubSub> {
    // Subscribing is an inode being held by one attempt: lost when the kernel may still have an item for it, and
    // forgotten when the kernel let go of it since.
    Subscribing {
        attempt: u64,
        lost: bool,
        forgotten: bool,
    },
    Held(Hold<P>),
    // Lost is an inode held through a client whose connection failed. The kernel may still hold its item, so a listing
    // hands over its attributes only once it is held again.
    Lost,
}

// Hold is a held inode: later publishes to it reach this mount.
struct Hold<P: PubSub> {
    // subscription holds the inode until it is dropped.
    _subscription: P::Subscription,
    // generation is the client the hold was made through.
    generation: u64,
    // since is the clock when the hold began, which also tells it apart from any other hold of the inode.
    since: u64,
    // replying counts this mount's writes that committed changes to the inode and have not replied yet.
    replying: u32,
    // confirmed is the highest version whose delivery this mount acknowledged, which a write may have committed.
    confirmed: u64,
    // pending counts deliveries still being handled, and pending_highest is the highest version among them.
    pending: u32,
    pending_highest: u64,
    // replies counts replies handed to the kernel cached with data older than a pending delivery, so a delivery
    // invalidates again after any that happened while it invalidated.
    replies: u64,
    // asked counts the deliveries invalidating now that have asked the kernel to drop its copy, and round and asked_at
    // are when the first of them asked, by the clock and in time. again counts the deliveries invalidating again.
    asked: u32,
    round: u64,
    asked_at: Option<Instant>,
    again: u32,
}

impl<P: PubSub> Inner<P> {
    fn generation(&self) -> Option<u64> {
        match &self.connection {
            Connection::Up { generation, .. } => Some(*generation),
            Connection::Down { .. } => None,
        }
    }

    fn held(&mut self, inode: Inode) -> Option<&mut Hold<P>> {
        let generation = self.generation()?;
        match self.holds.get_mut(&inode) {
            Some(Holding::Held(hold)) if hold.generation == generation => Some(hold),
            _ => None,
        }
    }
}

impl<P: PubSub> State<P> {
    fn lock(&self) -> MutexGuard<'_, Inner<P>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wake(&self) {
        self.changed.notify(usize::MAX);
    }
}

// Next is what hold does next for an inode: nothing more when it is held, wait for a hold another read is making,
// or make the hold, as the attempt it numbers.
enum Next<L> {
    Held(bool),
    Wait(L),
    Subscribe(u64),
}

// Waiting is what a write waits for before it prepares again: another write of this mount to finish with inodes it
// claims, or other writes in flight on inodes it changes to end.
enum Waiting {
    Claimed,
    InFlight(Vec<(Inode, Attributes)>, Pending),
}

// claimed is each of inodes another write than write claims.
fn claimed<P: PubSub>(inner: &Inner<P>, write: u64, inodes: &[Inode]) -> Vec<Inode> {
    let other = |inode: &Inode| inner.claims.get(inode).is_some_and(|owner| *owner != write);
    inodes.iter().copied().filter(other).collect()
}

// contended says whether a write whose operation the kernel's purge waits for must fail rather than wait on inodes:
// a delivery of one is being handled here, or of any inode when a purge may wait on others, whose purge the kernel
// may hold back until the write ends.
fn contended<P: PubSub>(
    inner: &mut Inner<P>,
    inodes: &[Inode],
    purge: Purge,
    kernel: &dyn KernelCache,
) -> bool {
    match purge {
        Purge::Waits | Purge::Unknown if kernel.purge_waits_on_others() => {
            let generation = inner.generation();
            inner.holds.values().any(|holding| {
                matches!(holding, Holding::Held(hold) if Some(hold.generation) == generation && hold.pending > 0)
            })
        }
        Purge::Waits | Purge::Unknown => inodes
            .iter()
            .any(|inode| inner.held(*inode).is_some_and(|hold| hold.pending > 0)),
        Purge::GoesAhead => false,
    }
}

// live says whether a write is in flight on attributes, whose pending change version is pending.
fn live(attributes: &Attributes, pending: Option<&ChangeVersion>) -> bool {
    pending.is_some_and(|pending| pending.number > attributes.change_version.number)
}

// read_pending reads the pending change version of each inode of inodes that has one: the version a write in flight
// will move it to, named by the mount making it, kept apart from the attributes, which hold committed state only. The
// write's commit, or a settle, moves the change version up to it and clears it.
async fn read_pending<T: ReadKeyValueTransaction>(
    metadata: &T,
    inodes: &[Inode],
) -> Result<Pending, IoFailure<T::Failure, Infallible>> {
    let mut pending = HashMap::new();
    for inode in inodes {
        if let Some(stored) = metadata
            .get(FsKey::new_pending_change_version(*inode).into())
            .await
            .map_err(IoFailure::Metadata)?
        {
            let version = ChangeVersion::view(&stored)
                .map_err(|corrupt| IoFailure::Corrupt(corrupt.into()))?;
            pending.insert(*inode, version);
        }
    }
    Ok(pending)
}

impl<P: PubSub<Message = u64>> StrongUntilDisconnectCached<P> {
    // client is the connected client, connecting one first when there is none.
    // waited is the first connect that may have run while this operation waited, on a hold another operation was
    // making or here: one that failed fails this operation too, rather than it trying again in turn.
    async fn client<St: Store>(
        &self,
        mount: &Mount<St>,
        mut waited: Option<u64>,
    ) -> Result<(Arc<P>, u64), Failure<P, St>> {
        let attempt = loop {
            let listener = {
                let mut inner = self.state.lock();
                match &inner.connection {
                    Connection::Up { client, generation } => {
                        return Ok((Arc::clone(client), *generation));
                    }
                    // An operation that waited for the purge could hold it up, since the purge waits for operations
                    // in flight on the items it drops, so it fails at once instead.
                    Connection::Down { purging: true, .. } => return Err(StrongFailure::Purging),
                    _ => {}
                }
                if let (Some(from), Some((failed, unconnected))) = (waited, &inner.unconnected)
                    && *failed >= from
                {
                    return Err(unconnected.clone().into());
                }
                match &inner.connection {
                    Connection::Down {
                        connecting: true, ..
                    } => {
                        waited = Some(waited.unwrap_or(inner.connects).min(inner.connects));
                        self.state.changed.listen()
                    }
                    _ => {
                        inner.connection = Connection::Down {
                            connecting: true,
                            purging: false,
                        };
                        inner.connects += 1;
                        break inner.connects;
                    }
                }
            };
            listener.await;
        };
        let connecting = mount.runtime.timeout(CONNECT_TIMEOUT, (self.connect)());
        let connected = match connecting.await {
            Some(connected) => connected.map_err(|failure| Unconnected::Failed(failure.into())),
            None => Err(Unconnected::TimedOut),
        };
        let mut inner = self.state.lock();
        // The watcher is started and recorded before the lock is let go of, so its own disconnect, which takes the
        // lock, finds it recorded.
        let generation = inner.generations + 1;
        let watched = connected.map(|client| {
            let client = Arc::new(client);
            let (state, kernel, runtime, watched) = (
                Arc::downgrade(&self.state),
                Arc::clone(&mount.kernel),
                Arc::clone(&mount.runtime),
                Arc::clone(&client),
            );
            let (stop, stopped) = AbortHandle::new_pair();
            mount.runtime.spawn(Box::pin(async move {
                let _ = Abortable::new(
                    async move {
                        let failure = watched.failed().await;
                        tracing::warn!(%failure, "the volume's pub/sub connection failed, so the kernel's cache is dropped");
                        if let Some(state) = state.upgrade() {
                            disconnected(&state, &*kernel, &*runtime, generation).await;
                        }
                    },
                    stopped,
                )
                .await;
            }));
            (client, stop)
        });
        let result = match watched {
            Ok((client, stop)) => {
                inner.generations = generation;
                inner.connection = Connection::Up {
                    client: Arc::clone(&client),
                    generation,
                };
                if let Some(earlier) = self
                    .state
                    .watching
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .replace(stop)
                {
                    earlier.abort();
                }
                Ok((client, generation))
            }
            Err(unconnected) => {
                inner.connection = Connection::Down {
                    connecting: false,
                    purging: false,
                };
                inner.unconnected = Some((attempt, unconnected.clone()));
                Err(unconnected.into())
            }
        };
        drop(inner);
        self.state.wake();
        result
    }

    // subscribe holds inode through client, for the hold attempt left Subscribing.
    async fn subscribe<St: Store>(
        &self,
        mount: &Mount<St>,
        inode: Inode,
        attempt: u64,
        waited: Option<u64>,
    ) -> Result<(), Failure<P, St>> {
        // A delivery reaches only the hold it was published to, once that hold has begun.
        let id = Arc::new(AtomicU64::new(0));
        let held = async {
            let (client, generation) = self.client(mount, waited).await?;
            // The handler holds the state weakly, so the client the state holds does not keep it alive.
            let (state, kernel, runtime, id) = (
                Arc::downgrade(&self.state),
                Arc::clone(&mount.kernel),
                Arc::clone(&mount.runtime),
                Arc::clone(&id),
            );
            let subscription = client
                .subscribe(Key(inode.0), move |version| {
                    let (state, kernel, runtime, id) = (
                        Weak::clone(&state),
                        Arc::clone(&kernel),
                        Arc::clone(&runtime),
                        Arc::clone(&id),
                    );
                    async move {
                        if let Some(state) = state.upgrade() {
                            delivered(&state, &*kernel, &runtime, inode, &id, version).await;
                        }
                    }
                })
                .await
                .map_err(StrongFailure::Hold)?;
            Ok((subscription, generation))
        }
        .await;
        let mut inner = self.state.lock();
        let subscribing = match inner.holds.get(&inode) {
            Some(Holding::Subscribing {
                attempt: made,
                lost,
                forgotten,
            }) if *made == attempt => Some((*lost, *forgotten)),
            _ => None,
        };
        let current = inner.generation();
        let result = match (held, subscribing) {
            // A hold lost or replaced while it was made is let go of, and the operation holds again.
            (Ok(_), None) => Ok(()),
            // So is one forgotten while it was made, which the kernel has no item for.
            (Ok(_), Some((_, true))) => {
                inner.holds.remove(&inode);
                Ok(())
            }
            (Ok((_, generation)), Some(_)) if current != Some(generation) => {
                inner.holds.insert(inode, Holding::Lost);
                Ok(())
            }
            (Ok((subscription, generation)), Some((_, false))) => {
                let since = self.state.clock.fetch_add(1, Ordering::SeqCst);
                id.store(since, Ordering::SeqCst);
                inner.holds.insert(
                    inode,
                    Holding::Held(Hold {
                        _subscription: subscription,
                        generation,
                        since,
                        replying: 0,
                        confirmed: 0,
                        pending: 0,
                        pending_highest: 0,
                        replies: 0,
                        asked: 0,
                        round: 0,
                        asked_at: None,
                        again: 0,
                    }),
                );
                Ok(())
            }
            // An inode lost stays lost until it is held again.
            (Err(failure), Some((lost, forgotten))) => {
                match lost && !forgotten {
                    true => inner.holds.insert(inode, Holding::Lost),
                    false => inner.holds.remove(&inode),
                };
                Err(failure)
            }
            (Err(failure), None) => Err(failure),
        };
        drop(inner);
        self.state.wake();
        result
    }

    // handling is the result handling of reads, as result_handling says, letting unheld inodes go when listed.
    #[allow(clippy::too_many_arguments)]
    fn handling(
        &self,
        mount: &Mount<impl Store>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        began: u64,
        purge: Purge,
        no_cache_honoured: bool,
        listed: bool,
    ) -> ResultHandling {
        let mut inner = self.state.lock();
        let mut replies = Vec::new();
        // behind says whether a read is older than a write this mount knows of, or shows a write in flight it may have
        // missed, which only that write's end lets it be cached past.
        let mut behind = false;
        for (inode, read) in reads {
            let Some(hold) = inner.held(*inode) else {
                // The kernel has an item for no inode this mount never held, nor let go of.
                if listed && !inner.holds.contains_key(inode) {
                    continue;
                }
                return ResultHandling::Retry;
            };
            let version = read.change_version.number;
            if hold.since >= ticket {
                return ResultHandling::Retry;
            }
            if version < hold.confirmed {
                behind = true;
                continue;
            }
            let pending_version = pending.get(inode);
            // A read showing a write in flight older than the delivery goes no further: that write may have finished.
            if hold.pending > 0
                && version < hold.pending_highest
                && (!live(read, pending_version)
                    || pending_version
                        .is_some_and(|pending| pending.number >= hold.pending_highest))
            {
                // The purge drops a read it waits for begun before the kernel was asked. Any other the kernel asks
                // again about goes uncached, as one behind does. One begun since, likely held back until the kernel
                // purged, waits for the round to end, at most LATE_READS_WAIT as the purge may wait for it. Any other,
                // such as a write's reply, which must go, makes the delivery invalidate again; then a read the purge
                // goes ahead of waits for the write, which waits on nothing here.
                match purge {
                    Purge::GoesAhead if hold.again > 0 => return ResultHandling::Retry,
                    Purge::Waits if hold.asked == 0 || began < hold.round => {}
                    _ if no_cache_honoured => behind = true,
                    Purge::Waits
                        if hold
                            .asked_at
                            .is_some_and(|asked| mount.runtime.now() - asked < LATE_READS_WAIT) =>
                    {
                        return ResultHandling::Retry;
                    }
                    Purge::GoesAhead | Purge::Waits | Purge::Unknown => replies.push(*inode),
                }
                continue;
            }
            behind |= live(read, pending_version);
        }
        // Waiting on a write while the kernel's purge may wait for this operation could hold up a purge that write
        // waits on, so the reply goes uncached instead: the read was taken while the operation ran, so it may be
        // returned, only not kept.
        if behind {
            return match purge {
                Purge::Waits | Purge::Unknown => ResultHandling::NoCache,
                Purge::GoesAhead => ResultHandling::Retry,
            };
        }
        for inode in replies {
            if let Some(hold) = inner.held(inode) {
                hold.replies += 1;
            }
        }
        ResultHandling::Cache
    }

    // replied lets the holds a write's reply held up go on: it has replied, or will not.
    fn replied(&self, prepared: &Prepared) {
        let mut inner = self.state.lock();
        for (inode, since) in &prepared.0.replying {
            if let Some(hold) = inner.held(*inode).filter(|hold| hold.since == *since) {
                hold.replying -= 1;
            }
        }
        drop(inner);
        self.state.wake();
    }

    // announce writes, for every inode of inodes, the version this write will move it to as its pending change
    // version, and returns the ones it wrote. An inode this write announced already keeps its announcement. When
    // another write is in flight on any of them, it writes nothing and returns their attributes and pending versions.
    #[allow(clippy::type_complexity)]
    async fn announce<St: Store>(
        &self,
        mount: &Mount<St>,
        inodes: &[Inode],
        announced: &HashMap<Inode, ChangeVersion>,
    ) -> Result<
        Result<Vec<(Inode, ChangeVersion)>, (Vec<(Inode, Attributes)>, Pending)>,
        Failure<P, St>,
    > {
        mount
            .store
            .metadata()
            .transact(|mut metadata| {
                let inodes = inodes.to_vec();
                let id = mount.id;
                Box::pin(async move {
                    let mut read = Vec::new();
                    for inode in inodes {
                        // An inode that vanished fails the write when it runs again.
                        if let Some(attributes) = read_attributes(&*metadata, inode)
                            .await
                            .map_err(StoreWork::Read)?
                        {
                            read.push((inode, attributes));
                        }
                    }
                    let inodes: Vec<Inode> = read.iter().map(|(inode, _)| *inode).collect();
                    let pending = read_pending(&*metadata, &inodes)
                        .await
                        .map_err(StoreWork::Read)?;
                    let in_flight: Vec<(Inode, Attributes)> = read
                        .iter()
                        .filter(|(inode, attributes)| {
                            live(attributes, pending.get(inode))
                                && announced.get(inode) != pending.get(inode)
                        })
                        .cloned()
                        .collect();
                    if !in_flight.is_empty() {
                        return Ok((Err((in_flight, pending)), metadata));
                    }
                    let mut fresh = Vec::new();
                    for (inode, attributes) in read {
                        if live(&attributes, pending.get(&inode)) {
                            continue;
                        }
                        let version = attributes.change_version.after(id);
                        fresh.push((inode, version));
                        metadata
                            .set(
                                FsKey::new_pending_change_version(inode).into(),
                                Bytes::from(&version),
                            )
                            .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                    }
                    Ok((Ok(fresh), metadata))
                })
            })
            .await
            .map_err(StrongFailure::Store)
    }

    // ended waits until no write of in_flight is still in flight, settling any that outlasted its writer. A write whose
    // operation the kernel's purge waits for fails at once while a delivery it is contended by is handled here: that
    // delivery's write waits for this mount to drop a copy, which the kernel may hold back until this write ends.
    async fn ended<St: Store>(
        &self,
        mount: &Mount<St>,
        mut in_flight: Vec<(Inode, Attributes)>,
        mut pending: Pending,
        purge: Purge,
    ) -> Result<(), Failure<P, St>> {
        loop {
            let inodes: Vec<Inode> = in_flight.iter().map(|(inode, _)| *inode).collect();
            if contended(&mut self.state.lock(), &inodes, purge, &*mount.kernel) {
                return Err(StrongFailure::Contended);
            }
            Cached::<St>::settled(self, mount, &in_flight, &pending).await?;
            let (stored, stored_pending) = self.stored(mount, &inodes).await?;
            in_flight = in_flight
                .into_iter()
                .zip(stored)
                .filter_map(|((inode, _), stored)| {
                    let version = stored_pending.get(&inode);
                    stored
                        .filter(|stored| live(stored, version) && version == pending.get(&inode))
                        .map(|stored| (inode, stored))
                })
                .collect();
            pending = stored_pending;
            if in_flight.is_empty() {
                return Ok(());
            }
        }
    }

    // stored reads the attributes of each inode of inodes, None for one that does not exist, and their pending change
    // versions.
    async fn stored<St: Store>(
        &self,
        mount: &Mount<St>,
        inodes: &[Inode],
    ) -> Result<(Vec<Option<Attributes>>, Pending), Failure<P, St>> {
        mount
            .store
            .metadata()
            .read_transact(|metadata| {
                Box::pin(async move {
                    let mut stored = Vec::new();
                    for inode in inodes {
                        stored.push(
                            read_attributes(&*metadata, *inode)
                                .await
                                .map_err(StoreWork::Read)?,
                        );
                    }
                    let pending = read_pending(&*metadata, inodes)
                        .await
                        .map_err(StoreWork::Read)?;
                    Ok((stored, pending))
                })
            })
            .await
            .map_err(StrongFailure::Store)
    }

    // claim claims every inode of inodes for write, or none of them when another write of this mount claims one, and
    // says whether it did.
    fn claim(&self, write: u64, inodes: &[Inode]) -> bool {
        let mut inner = self.state.lock();
        if !claimed(&inner, write, inodes).is_empty() {
            return false;
        }
        for inode in inodes {
            inner.claims.insert(*inode, write);
        }
        true
    }

    // unclaimed waits until no other write of this mount claims any of inodes, and fails at once while the mount
    // purges, as client does. A write the kernel's purge waits for asks one running its own delivery to yield, since
    // that delivery's purge may wait on this write's operation.
    async fn unclaimed<St: Store>(
        &self,
        write: u64,
        inodes: &[Inode],
        purge: Purge,
    ) -> Result<(), Failure<P, St>> {
        loop {
            let listener = {
                let mut inner = self.state.lock();
                let claimed = claimed(&inner, write, inodes);
                if claimed.is_empty() {
                    return Ok(());
                }
                if let Connection::Down { purging: true, .. } = inner.connection {
                    return Err(StrongFailure::Purging);
                }
                if let Purge::Waits | Purge::Unknown = purge {
                    let Inner {
                        claims, delivering, ..
                    } = &mut *inner;
                    for owner in claimed.iter().filter_map(|inode| claims.get(inode)) {
                        if let Some(asked @ false) = delivering.get_mut(owner) {
                            *asked = true;
                            self.state.wake();
                        }
                    }
                }
                self.state.changed.listen()
            };
            listener.await;
        }
    }

    // own_delivered waits until the own delivery of write ends, and fails once a write of this mount asks it to yield.
    async fn own_delivered<St: Store>(&self, write: u64) -> Result<(), Failure<P, St>> {
        loop {
            let listener = match self.state.lock().delivering.get(&write) {
                None => return Ok(()),
                Some(true) => return Err(StrongFailure::Contended),
                Some(false) => self.state.changed.listen(),
            };
            listener.await;
        }
    }

    // release lets go of the holds of inodes, under the lock, so no hold of the same key is made through the client
    // before the old one is gone. One still being made is let go of once it is made.
    fn release(&self, inodes: &[Inode]) {
        {
            let mut inner = self.state.lock();
            for inode in inodes {
                match inner.holds.get_mut(inode) {
                    Some(Holding::Subscribing { forgotten, .. }) => *forgotten = true,
                    _ => {
                        inner.holds.remove(inode);
                    }
                }
            }
        }
        self.state.wake();
    }

    // unclaim lets go of every inode write claims.
    fn unclaim(&self, write: u64) {
        self.state.lock().claims.retain(|_, owner| *owner != write);
        self.state.wake();
    }

    // floor keeps this mount's reads of each inode of announced from going behind the write announcing it here, which
    // no delivery tells this mount of.
    fn floor(&self, announced: &HashMap<Inode, ChangeVersion>) {
        let mut inner = self.state.lock();
        for (inode, version) in announced {
            if let Some(hold) = inner.held(*inode) {
                hold.confirmed = hold.confirmed.max(version.number);
            }
        }
    }

    // settle ends writes in flight that no write will commit: every inode whose pending change version is still one of
    // announced moves to it with nothing else changed, which is what any mount told of the write waits for, and loses
    // its pending change version.
    async fn settle<St: Store>(
        &self,
        mount: &Mount<St>,
        announced: &HashMap<Inode, ChangeVersion>,
    ) -> Result<(), Failure<P, St>> {
        settle(mount, announced).await.map_err(StrongFailure::Store)
    }
}

// settle is StrongUntilDisconnectCached::settle, for any mount.
async fn settle<St: Store>(
    mount: &Mount<St>,
    announced: &HashMap<Inode, ChangeVersion>,
) -> Result<(), Settled<St>> {
    if announced.is_empty() {
        return Ok(());
    }
    mount
        .store
        .metadata()
        .transact(|mut metadata| {
            let announced = announced.clone();
            Box::pin(async move {
                for (inode, version) in announced {
                    let Some(mut attributes) = read_attributes(&*metadata, inode)
                        .await
                        .map_err(StoreWork::Read)?
                    else {
                        continue;
                    };
                    let pending = read_pending(&*metadata, &[inode])
                        .await
                        .map_err(StoreWork::Read)?;
                    let pending = pending.get(&inode);
                    if live(&attributes, pending) && pending == Some(&version) {
                        attributes.change_version = version;
                        metadata
                            .set(
                                FsKey::new_attributes(inode).into(),
                                Bytes::from(&attributes),
                            )
                            .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                        metadata
                            .clear(FsKey::new_pending_change_version(inode).into())
                            .map_err(|failure| StoreWork::Write(StoreError::Failed(failure)))?;
                    }
                }
                Ok(((), metadata))
            })
        })
        .await
}

// delivered handles a publish of version to the hold of inode whose id is in hold, and then acknowledges it by
// returning. A purge keeps this mount's holds until it has dropped the kernel's copies, so a delivery is acknowledged
// only once no purge is running.
async fn delivered<P: PubSub>(
    state: &Arc<State<P>>,
    kernel: &dyn KernelCache,
    runtime: &Arc<dyn Runtime>,
    inode: Inode,
    hold: &AtomicU64,
    version: u64,
) {
    // A hold's id is set as the hold begins. A delivery before then finds nothing cached through it to drop, and any
    // read through it comes after the write's announcement, so it sees that write.
    match hold.load(Ordering::SeqCst) {
        0 => {}
        id => handled(state, kernel, runtime, inode, id, version).await,
    }
    purged(state).await;
}

// Round is how a delivery's round of invalidation ended: the delivery acknowledged, it goes again since a reply of
// older data came during the round, it waits for this mount's own write to the inode to reply first, or its hold is
// gone.
enum Round {
    Acknowledged,
    Again,
    Replying,
    Gone,
}

// end_round ends a round of the delivery of version to inode's hold id, begun when the hold had counted replies, of a
// delivery that invalidates again when again says so.
fn end_round<P: PubSub>(
    inner: &mut Inner<P>,
    inode: Inode,
    id: u64,
    replies: u64,
    again: bool,
    version: u64,
) -> Round {
    let Some(hold) = inner.held(inode).filter(|hold| hold.since == id) else {
        return Round::Gone;
    };
    hold.asked -= 1;
    if hold.asked == 0 {
        hold.asked_at = None;
    }
    if hold.replies != replies {
        if !again {
            hold.again += 1;
        }
        return Round::Again;
    }
    if hold.replying > 0 {
        return Round::Replying;
    }
    if again {
        hold.again -= 1;
    }
    hold.pending -= 1;
    hold.confirmed = hold.confirmed.max(version);
    if hold.pending == 0 {
        hold.pending_highest = 0;
    }
    Round::Acknowledged
}

// purged resolves once no purge is running.
async fn purged<P: PubSub>(state: &State<P>) {
    loop {
        let listener = {
            let inner = state.lock();
            if !matches!(inner.connection, Connection::Down { purging: true, .. }) {
                return;
            }
            state.changed.listen()
        };
        listener.await;
    }
}

// handled drops the kernel's copy of inode for a publish of version to its hold id, again until no reply of older data
// happened while it did and none of this mount's writes to the inode is still to reply.
async fn handled<P: PubSub>(
    state: &Arc<State<P>>,
    kernel: &dyn KernelCache,
    runtime: &Arc<dyn Runtime>,
    inode: Inode,
    id: u64,
    version: u64,
) {
    let generation = {
        let mut inner = state.lock();
        let Some(hold) = inner.held(inode).filter(|hold| hold.since == id) else {
            return;
        };
        // A delivery no newer than one this mount acknowledged, or than a write of its own announced here, has nothing
        // left to drop: reads older than that already go no further.
        if version <= hold.confirmed {
            return;
        }
        hold.pending += 1;
        hold.pending_highest = hold.pending_highest.max(version);
        hold.generation
    };
    state.wake();
    // again says whether this delivery invalidates again, which the hold counts.
    let mut again = false;
    loop {
        let replies = match state.lock().held(inode).filter(|hold| hold.since == id) {
            Some(hold) => hold.replies,
            None => break,
        };
        // The hold notes when the kernel was asked, since the purge drops replies of the operations it waits for begun
        // before then. The round ends under the lock as the kernel answers, so no reply comes between purge and check.
        let ended = Arc::new(Mutex::new(None));
        let (asked, answered): (Asked, Answered) = {
            let (asking, clock, answering, ending) = (
                Arc::downgrade(state),
                Arc::clone(runtime),
                Arc::downgrade(state),
                Arc::clone(&ended),
            );
            (
                Box::new(move || {
                    if let Some(state) = asking.upgrade() {
                        let now = state.clock.fetch_add(1, Ordering::SeqCst);
                        if let Some(hold) = state.lock().held(inode).filter(|hold| hold.since == id)
                        {
                            if hold.asked == 0 {
                                hold.round = now;
                                hold.asked_at = Some(clock.now());
                            }
                            hold.asked += 1;
                        }
                    }
                }),
                Box::new(move |dropped| {
                    if let Some(state) = answering.upgrade()
                        && dropped
                    {
                        let round =
                            end_round(&mut state.lock(), inode, id, replies, again, version);
                        *ending.lock().unwrap_or_else(PoisonError::into_inner) = Some(round);
                    }
                }),
            )
        };
        let round = match kernel.invalidate_noting(inode, asked, answered).await {
            Ok(()) => ended
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .unwrap_or(Round::Gone),
            // A copy the kernel keeps cannot be acknowledged as dropped. When a second try fails too, the mount drops
            // the connection the hold was made through, so the coordinator stops counting it as a holder.
            Err(_) => match kernel.invalidate(inode).await {
                Ok(()) => end_round(&mut state.lock(), inode, id, replies, again, version),
                Err(error) => {
                    tracing::warn!(inode = inode.0, %error, "invalidating the kernel's copy failed, so the pub/sub connection is dropped");
                    disconnected(state, kernel, &**runtime, generation).await;
                    break;
                }
            },
        };
        match round {
            Round::Acknowledged | Round::Gone => break,
            Round::Again => again = true,
            // A write of this mount committed a change to the inode and has yet to hand it over, which it does
            // without waiting on this delivery, so the delivery invalidates again once it has.
            Round::Replying => {
                let listener = {
                    let mut inner = state.lock();
                    inner
                        .held(inode)
                        .filter(|hold| hold.since == id && hold.replying > 0)
                        .map(|_| state.changed.listen())
                };
                if let Some(listener) = listener {
                    listener.await;
                }
            }
        }
    }
    state.wake();
}

// disconnected drops everything held through client generation, whose connection failed, and the kernel's copy of
// every inode it held, so the mount serves nothing it may have missed a write to. Each inode it held is kept as lost.
async fn disconnected<P: PubSub>(
    state: &State<P>,
    kernel: &dyn KernelCache,
    runtime: &dyn Runtime,
    generation: u64,
) {
    let dropped: Vec<(Inode, Holding<P>)> = {
        let mut inner = state.lock();
        if inner.generation() != Some(generation) {
            return;
        }
        inner.connection = Connection::Down {
            connecting: false,
            purging: true,
        };
        inner.seen.clear();
        let mut dropped = Vec::new();
        // An inode still being held is kept as lost only if it was lost before: the kernel has no item for any other.
        inner.holds.retain(|inode, holding| {
            let lost = match holding {
                Holding::Held(_) => {
                    dropped.push((*inode, std::mem::replace(holding, Holding::Lost)));
                    true
                }
                Holding::Subscribing {
                    lost, forgotten, ..
                } => *lost && !*forgotten,
                Holding::Lost => true,
            };
            if lost {
                *holding = Holding::Lost;
            }
            lost
        });
        // The purges go in order, so a mount purges the same way each time it runs.
        dropped.sort_by_key(|(inode, _)| inode.0);
        dropped
    };
    state.wake();
    // The holds go only once the purge is done, so the coordinator counts this mount as a holder of each inode while
    // the kernel may still have a copy of it.
    let (dropped, holds): (Vec<Inode>, Vec<Holding<P>>) = dropped.into_iter().unzip();
    join_all(dropped.iter().map(|inode| purge(runtime, kernel, *inode))).await;
    // The watcher of the failed client is stopped only now, since this may be it. No client connects while purging,
    // so the watcher is still that client's.
    let watching = state
        .watching
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    drop(holds);
    if let Connection::Down { purging, .. } = &mut state.lock().connection {
        *purging = false;
    }
    state.wake();
    if let Some(watching) = watching {
        watching.abort();
    }
}

// purge drops the kernel's copy of inode for a purge, trying again until the kernel does: a copy the kernel keeps
// cannot be let go of, so the purge, and with it the mount, waits instead.
async fn purge(runtime: &dyn Runtime, kernel: &dyn KernelCache, inode: Inode) {
    let mut wait = PURGE_RETRY;
    while let Err(error) = kernel.invalidate(inode).await {
        tracing::warn!(inode = inode.0, %error, "dropping the kernel's copy after the connection failed failed, so it is tried again");
        runtime.sleep(wait).await;
        wait = (wait * 2).min(PURGE_RETRY_MOST);
    }
}

// Dropping the consistency stops watching its client, which it no longer holds, so the connection closes.
impl<P: PubSub> Drop for StrongUntilDisconnectCached<P> {
    fn drop(&mut self) {
        if let Some(watching) = self
            .state
            .watching
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            watching.abort();
        }
    }
}

impl<St: Store, P: PubSub<Message = u64>> Cached<St> for StrongUntilDisconnectCached<P> {
    type Failure = Failure<P, St>;
    type Prepared = Prepared;
    type Ticket = u64;
    type InodeState = Pending;

    async fn inode_state<T: ReadKeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        metadata: &T,
        inodes: &[Inode],
    ) -> Result<Pending, Self::Failure> {
        read_pending(metadata, inodes)
            .await
            .map_err(StrongFailure::Pending)
    }

    // may_commit lets a write commit once it moves every inode it changes to the version it announced and published
    // and, if prepared, still holds each through the hold it marked, so a delivery after the commit waits for its
    // reply. A new inode needs no announcement: no mount can hold it before the commit that names it. The commit clears
    // the pending change versions it takes up.
    async fn may_commit<T: KeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        _: &Mount<St>,
        prepared: Option<&Prepared>,
        changes: &HashMap<Inode, Change>,
        metadata: &mut T,
    ) -> Result<bool, Self::Failure> {
        let inodes: Vec<Inode> = changes.keys().copied().collect();
        let pending = read_pending(&*metadata, &inodes)
            .await
            .map_err(StrongFailure::Pending)?;
        let published = |inode: &Inode, version: ChangeVersion| {
            prepared.is_some_and(|prepared| prepared.0.announced.get(inode) == Some(&version))
        };
        let may_commit = {
            let mut inner = self.state.lock();
            let mut marked = |inode: Inode| {
                prepared.is_none_or(|prepared| {
                    inner
                        .held(inode)
                        .is_some_and(|hold| prepared.0.replying.contains(&(inode, hold.since)))
                })
            };
            changes.iter().all(|(inode, change)| {
                marked(*inode)
                    && match change {
                        Change::Written(after) => {
                            after.change_version.number == 0
                                || (pending.get(inode) == Some(&after.change_version)
                                    && published(inode, after.change_version))
                        }
                        Change::Deleted(before) => pending.get(inode).is_some_and(|version| {
                            live(before, Some(version)) && published(inode, *version)
                        }),
                    }
            })
        };
        if may_commit {
            for inode in pending.keys() {
                metadata
                    .clear(FsKey::new_pending_change_version(*inode).into())
                    .map_err(|failure| StrongFailure::Pending(IoFailure::Metadata(failure)))?;
            }
        }
        Ok(may_commit)
    }

    // prepare claims and holds every inode the write changes, new ones too so its reply hands over only held inodes,
    // then announces and publishes the version each existing one moves to. Before waiting on another write in flight,
    // it lets go of what it claimed and announced, so no two writes wait for each other, and it does so and fails when
    // a write its own delivery's purge may wait on asks it to yield.
    async fn prepare(
        &self,
        mount: &Mount<St>,
        prepared: Option<Prepared>,
        changes: HashMap<Inode, Change>,
        purge: Purge,
    ) -> Result<Prepared, Self::Failure> {
        let (write, mut announced) = match prepared {
            Some(prepared) => {
                self.replied(&prepared);
                (prepared.0.write, prepared.0.announced.clone())
            }
            None => (
                self.state.clock.fetch_add(1, Ordering::SeqCst),
                HashMap::new(),
            ),
        };
        // The inodes go in order, so a write holds, announces and publishes them the same way each time it runs.
        let mut changed: Vec<Inode> = changes.keys().copied().collect();
        changed.sort_by_key(|inode| inode.0);
        let of = |kind: fn(&Change) -> bool| -> Vec<Inode> {
            changes
                .iter()
                .filter(|(_, change)| kind(change))
                .map(|(inode, _)| *inode)
                .collect()
        };
        let new = of(
            |change| matches!(change, Change::Written(after) if after.change_version.number == 0),
        );
        let deleted = of(|change| matches!(change, Change::Deleted(_)));
        let existing: Vec<Inode> = changed
            .iter()
            .filter(|inode| !new.contains(inode))
            .copied()
            .collect();
        loop {
            let readied = async {
                if !self.claim(write, &changed) {
                    return Ok(Some(Waiting::Claimed));
                }
                self.hold(mount, &changed, 0).await?;
                let (client, _) = self.client(mount, None).await?;
                let fresh = match self.announce(mount, &existing, &announced).await? {
                    Ok(fresh) => fresh,
                    Err((in_flight, pending)) => {
                        return Ok(Some(Waiting::InFlight(in_flight, pending)));
                    }
                };
                announced.extend(fresh.iter().map(|(inode, version)| (*inode, *version)));
                // No delivery tells this mount of its own write. It drops this mount's copies as one would if the purge
                // goes ahead of it; one that would wait for it cannot, so the floor keeps reads behind it from being
                // cached, and only the kernel's copy of the attributes goes, which waits for nothing.
                let mut own = Vec::new();
                match purge {
                    Purge::GoesAhead => {
                        let mut inner = self.state.lock();
                        for (inode, version) in &fresh {
                            if let Some(hold) = inner.held(*inode) {
                                own.push((*inode, hold.since, version.number));
                            }
                        }
                    }
                    Purge::Waits | Purge::Unknown => {
                        self.floor(&announced);
                        for (inode, _) in &fresh {
                            mount
                                .kernel
                                .invalidate_attributes(*inode)
                                .map_err(StrongFailure::Uninvalidated)?;
                        }
                    }
                }
                // The own delivery runs on its own, so a write that yields leaves its purge, which no one can cancel.
                if !own.is_empty() {
                    self.state.lock().delivering.insert(write, false);
                    self.state.wake();
                    let (state, kernel, runtime) = (
                        Arc::clone(&self.state),
                        Arc::clone(&mount.kernel),
                        Arc::clone(&mount.runtime),
                    );
                    mount.runtime.spawn(Box::pin(async move {
                        join_all(own.into_iter().map(|(inode, id, version)| {
                            handled(&state, &*kernel, &runtime, inode, id, version)
                        }))
                        .await;
                        state.lock().delivering.remove(&write);
                        state.wake();
                    }));
                }
                let published = join_all(fresh.iter().map(|(inode, version)| {
                    let client = Arc::clone(&client);
                    let (inode, number) = (*inode, version.number);
                    async move { client.publish(Key(inode.0), &number, self.timeout).await }
                }));
                if let Some(failure) = published.await.into_iter().find_map(Result::err) {
                    return Err(StrongFailure::Publish(failure));
                }
                self.own_delivered::<St>(write).await.map(|()| None)
            }
            .await;
            let waiting = match readied {
                Ok(None) => break,
                Ok(Some(waiting)) => waiting,
                // Nothing of the write committed, so neither its new inodes nor the one it deletes get an item.
                Err(failure) => {
                    let settled = self.settle(mount, &announced).await;
                    self.release(&new);
                    self.release(&deleted);
                    self.unclaim(write);
                    settled?;
                    return Err(failure);
                }
            };
            let settled = self.settle(mount, &announced).await;
            announced.clear();
            self.unclaim(write);
            let waited = async {
                settled?;
                match waiting {
                    Waiting::Claimed => self.unclaimed::<St>(write, &changed, purge).await?,
                    Waiting::InFlight(in_flight, pending) => {
                        self.ended(mount, in_flight, pending, purge).await?;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(failure) = waited {
                self.release(&new);
                self.release(&deleted);
                return Err(failure);
            }
        }
        let replying = {
            let mut inner = self.state.lock();
            changed
                .iter()
                .filter_map(|inode| {
                    let hold = inner.held(*inode)?;
                    hold.replying += 1;
                    Some((*inode, hold.since))
                })
                .collect()
        };
        Ok(Prepared(Arc::new(Readied {
            write,
            announced,
            new,
            deleted,
            replying,
        })))
    }

    // finish settles every announcement the write left that no commit of it took up: all of them when it did not
    // commit, and those of inodes an earlier attempt changed but the committed one did not.
    async fn finish(
        &self,
        mount: &Mount<St>,
        prepared: Prepared,
        committed: Option<&HashMap<Inode, Change>>,
    ) {
        self.replied(&prepared);
        // The kernel never gets an item again for an inode a write deletes, which it let go of before, nor for a new
        // inode the write did not make, so nothing else lets go of their holds.
        let mut released = prepared.0.deleted.clone();
        match committed {
            Some(changes) => released.extend(
                changes
                    .iter()
                    .filter(|(_, change)| matches!(change, Change::Deleted(_)))
                    .map(|(inode, _)| *inode),
            ),
            // A write that failed may have committed when only the store's answer was lost, so only the new inodes
            // the store does not have are let go of.
            None => match self.stored(mount, &prepared.0.new).await {
                Ok((stored, _)) => released.extend(
                    prepared
                        .0
                        .new
                        .iter()
                        .zip(stored)
                        .filter(|(_, stored)| stored.is_none())
                        .map(|(inode, _)| *inode),
                ),
                Err(error) => {
                    tracing::warn!(%error, "reading the new inodes of a write that did not commit failed, so their holds stay");
                }
            },
        }
        self.release(&released);
        let left: HashMap<Inode, ChangeVersion> = prepared
            .0
            .announced
            .iter()
            .filter(|(inode, _)| committed.is_none_or(|changes| !changes.contains_key(inode)))
            .map(|(inode, version)| (*inode, *version))
            .collect();
        if let Err(error) = self.settle(mount, &left).await {
            tracing::warn!(%error, "settling what a write announced and did not commit failed");
        }
        self.unclaim(prepared.0.write);
    }

    fn reading(&self) -> u64 {
        self.state.clock.fetch_add(1, Ordering::SeqCst)
    }

    async fn hold(
        &self,
        mount: &Mount<St>,
        inodes: &[Inode],
        ticket: u64,
    ) -> Result<bool, Self::Failure> {
        let mut held_before = true;
        // waited is the first connect that may have run while this operation waited on a hold another was making.
        let mut waited = None;
        for &inode in inodes {
            loop {
                let next = {
                    let mut inner = self.state.lock();
                    if let Some(hold) = inner.held(inode) {
                        Next::Held(hold.since < ticket)
                    } else if let Some(Holding::Subscribing { .. }) = inner.holds.get(&inode) {
                        let from = inner.connects
                            + u64::from(!matches!(
                                inner.connection,
                                Connection::Down {
                                    connecting: true,
                                    ..
                                }
                            ));
                        waited = Some(waited.unwrap_or(from).min(from));
                        Next::Wait(self.state.changed.listen())
                    } else {
                        // A hold through a client that failed is lost too.
                        let lost = inner.holds.contains_key(&inode);
                        let attempt = self.state.clock.fetch_add(1, Ordering::SeqCst);
                        inner.holds.insert(
                            inode,
                            Holding::Subscribing {
                                attempt,
                                lost,
                                forgotten: false,
                            },
                        );
                        Next::Subscribe(attempt)
                    }
                };
                match next {
                    Next::Held(before) => {
                        held_before &= before;
                        break;
                    }
                    Next::Wait(listener) => listener.await,
                    Next::Subscribe(attempt) => {
                        self.subscribe(mount, inode, attempt, waited).await?;
                        held_before = false;
                        break;
                    }
                }
            }
        }
        Ok(held_before)
    }

    // result_handling has the kernel cache a read taken after its inodes were held, not older than any write this mount
    // acknowledged, and with no write in flight it may have missed. One failing only the last two is NoCache when the
    // purge may wait for its operation. A read older than a delivery being handled may go when the purge waits, or
    // NoCache when no_cache_honoured.
    fn result_handling(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        began: u64,
        purge: Purge,
        no_cache_honoured: bool,
    ) -> ResultHandling {
        self.handling(
            mount,
            reads,
            pending,
            ticket,
            began,
            purge,
            no_cache_honoured,
            false,
        )
    }

    // result_handling_listed handles the inodes of a listing held already as result_handling does, and lets the others
    // go, but for lost ones, which settled holds again.
    fn result_handling_listed(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
    ) -> ResultHandling {
        self.handling(
            mount,
            reads,
            pending,
            ticket,
            ticket,
            Purge::Unknown,
            false,
            true,
        )
    }

    async fn settled(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
    ) -> Result<(), Self::Failure> {
        let listener = self.state.changed.listen();
        let mut stale = HashMap::new();
        {
            let mut inner = self.state.lock();
            // A write in flight not met again for twice the wait is long over, settled by some read or by its writer.
            let waits = self.timeout + SETTLE_MARGIN;
            let now = mount.runtime.now();
            inner.seen.retain(|_, first| now - *first < waits * 2);
            for (inode, read) in reads {
                let Some(version) = pending
                    .get(inode)
                    .filter(|version| live(read, Some(version)))
                else {
                    continue;
                };
                let first = *inner
                    .seen
                    .entry((*inode, version.mount, version.number))
                    .or_insert(now);
                if now - first > waits {
                    stale.insert(*inode, *version);
                }
            }
        }
        if !stale.is_empty() {
            return self.settle(mount, &stale).await;
        }
        // Inodes the kernel may have an item for that are not held now, such as lost ones, are held again.
        let lost: Vec<Inode> = {
            let mut inner = self.state.lock();
            reads
                .iter()
                .map(|(inode, _)| *inode)
                .filter(|inode| inner.holds.contains_key(inode) && inner.held(*inode).is_none())
                .collect()
        };
        if !lost.is_empty() {
            return self.hold(mount, &lost, 0).await.map(|_| ());
        }
        let _ = select(listener, mount.runtime.sleep(RECHECK)).await;
        Ok(())
    }

    async fn reread(
        &self,
        mount: &Mount<St>,
        committed: &[(Inode, Attributes)],
    ) -> Result<(Vec<(Inode, Attributes)>, Pending), Self::Failure> {
        let inodes: Vec<Inode> = committed.iter().map(|(inode, _)| *inode).collect();
        let (stored, pending) = self.stored(mount, &inodes).await?;
        let reread = committed
            .iter()
            .zip(stored)
            .map(|((inode, written), stored)| match stored {
                Some(stored) if stored.change_version == written.change_version => {
                    Ok((*inode, stored))
                }
                _ => Err(StrongFailure::Overtaken),
            })
            .collect::<Result<_, _>>()?;
        Ok((reread, pending))
    }

    fn cached(&self, _: &Mount<St>, _: Inode, _: &Attributes) {}

    fn forget(&self, _: &Mount<St>, inode: Inode) {
        self.release(&[inode]);
    }

    fn write_back(&self) -> bool {
        false
    }
}
